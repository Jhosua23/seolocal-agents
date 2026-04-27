"""
Agent 26 — Pipeline Monitor
Phase 2 — Conversion & Retention Engine
AgentCore ID: pipelineMonitor

Purpose
-------
Runs every 4 hours (EventBridge cron) and on-demand. Pulls all KPIs across
GHL Sales / Fulfillment / Resurrection pipelines, Recurly MRR / churn, and
GHL lead-acquisition counters. Scores each metric GREEN / YELLOW / RED
against Gold Standard thresholds stored in S3, then posts a Block Kit
report card to the #c-suite-command-center Slack channel. RED metrics also
trigger a DM to Chuck.

Endpoints
---------
POST /monitor/pipeline   -> run full KPI check now
POST /ping               -> health check

No gRPC. No hardcoded credentials. Secrets via AWS SSM Parameter Store.
"""

import json
import logging
import os
import sys
import time
import traceback
from datetime import datetime, timedelta, timezone

import boto3
import requests
from botocore.exceptions import ClientError
from bedrock_agentcore.runtime import BedrockAgentCoreApp

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("pipelineMonitor")

app = BedrockAgentCoreApp()

# ---------------------------------------------------------------------------
# AWS clients
# ---------------------------------------------------------------------------
REGION = os.environ.get("AWS_REGION", "us-east-1")
_ssm = boto3.client("ssm", region_name=REGION)
_s3 = boto3.client("s3", region_name=REGION)

# ---------------------------------------------------------------------------
# SSM cache
# ---------------------------------------------------------------------------
_SSM_CACHE: dict = {}


def get_ssm(name: str, required: bool = True, default: str = "") -> str:
    if name in _SSM_CACHE:
        return _SSM_CACHE[name]
    try:
        r = _ssm.get_parameter(Name=name, WithDecryption=True)
        v = r["Parameter"]["Value"]
        _SSM_CACHE[name] = v
        return v
    except ClientError as e:
        if e.response["Error"]["Code"] == "ParameterNotFound":
            if required:
                raise RuntimeError(f"Required SSM parameter missing: {name}")
            _SSM_CACHE[name] = default
            return default
        raise


# ---------------------------------------------------------------------------
# Defaults — used if gold_standards.json not in S3
# ---------------------------------------------------------------------------
DEFAULT_GOLD_STANDARDS = {
    "speed_to_lead_minutes": {"green": 5, "yellow": 15, "red": 30},
    "show_rate_pct": {"green": 70, "yellow": 55, "red": 40},
    "close_rate_pct": {"green": 30, "yellow": 20, "red": 10},
    "intake_completion_pct": {"green": 85, "yellow": 70, "red": 55},
    "mrr_growth_pct": {"green": 10, "yellow": 3, "red": 0},
    "churn_risk_contacts": {"green": 2, "yellow": 5, "red": 10},
    "avg_client_tenure_months": {"green": 9, "yellow": 6, "red": 3},
    "leads_this_week": {"green": 50, "yellow": 30, "red": 15},
}

# ---------------------------------------------------------------------------
# GHL helpers
# ---------------------------------------------------------------------------
GHL_BASE = "https://services.leadconnectorhq.com"
GHL_VERSION = "2021-07-28"


def _ghl_headers() -> dict:
    return {
        "Authorization": f"Bearer {get_ssm('GHL_API_KEY')}",
        "Version": GHL_VERSION,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def ghl_pipeline_stages(pipeline_id: str) -> list:
    """Returns list of stages [{id, name}] for given pipeline."""
    url = f"{GHL_BASE}/opportunities/pipelines"
    location_id = get_ssm("GHL_LOCATION_ID", required=False) or get_ssm(
        "LOCATION_ID", required=False
    )
    params = {"locationId": location_id} if location_id else {}
    r = requests.get(url, headers=_ghl_headers(), params=params, timeout=20)
    if r.status_code >= 400:
        log.warning("ghl_pipeline_stages %s: %s", r.status_code, r.text[:300])
        return []
    pipelines = r.json().get("pipelines", [])
    for p in pipelines:
        if p.get("id") == pipeline_id:
            return p.get("stages", [])
    return []


def ghl_count_opportunities_in_stage(pipeline_id: str, stage_id: str) -> int:
    """Count open opportunities in a specific stage."""
    url = f"{GHL_BASE}/opportunities/search"
    location_id = get_ssm("GHL_LOCATION_ID", required=False) or get_ssm(
        "LOCATION_ID", required=False
    )
    params = {
        "location_id": location_id,
        "pipeline_id": pipeline_id,
        "pipeline_stage_id": stage_id,
        "limit": 100,
    }
    total = 0
    page_token = None
    pages = 0
    while True:
        if page_token:
            params["startAfterId"] = page_token
        try:
            r = requests.get(
                url, headers=_ghl_headers(), params=params, timeout=20
            )
        except requests.RequestException as e:
            log.warning("ghl_count_opportunities_in_stage error: %s", e)
            break
        if r.status_code >= 400:
            log.warning(
                "ghl_count_opportunities_in_stage %s: %s",
                r.status_code,
                r.text[:300],
            )
            break
        data = r.json()
        meta = data.get("meta") or {}
        opps = data.get("opportunities", [])
        total += len(opps)
        pages += 1
        next_token = meta.get("nextPageUrl") or meta.get("startAfterId")
        if not next_token or pages >= 20:
            # Try meta.total if provided
            reported_total = meta.get("total")
            if isinstance(reported_total, int):
                return reported_total
            break
        page_token = next_token
    return total


def ghl_contacts_by_tag(tag: str, limit: int = 100) -> int:
    """Count contacts with given tag. Returns best-effort count."""
    url = f"{GHL_BASE}/contacts/search"
    location_id = get_ssm("GHL_LOCATION_ID", required=False) or get_ssm(
        "LOCATION_ID", required=False
    )
    body = {
        "locationId": location_id,
        "pageLimit": limit,
        "filters": [{"field": "tags", "operator": "contains", "value": tag}],
    }
    try:
        r = requests.post(url, headers=_ghl_headers(), json=body, timeout=20)
    except requests.RequestException as e:
        log.warning("ghl_contacts_by_tag error: %s", e)
        return 0
    if r.status_code >= 400:
        log.warning(
            "ghl_contacts_by_tag %s: %s", r.status_code, r.text[:300]
        )
        return 0
    data = r.json()
    total = data.get("total")
    if isinstance(total, int):
        return total
    return len(data.get("contacts", []))


# ---------------------------------------------------------------------------
# Recurly helpers (best-effort — fall back to zeros if unavailable)
# ---------------------------------------------------------------------------
RECURLY_BASE = "https://v3.recurly.com"


def _recurly_headers() -> dict:
    import base64

    key = get_ssm("RECURLY_API_KEY", required=False)
    if not key:
        return {}
    auth = base64.b64encode(f"{key}:".encode()).decode()
    return {
        "Authorization": f"Basic {auth}",
        "Accept": "application/vnd.recurly.v2021-02-25+json",
        "Content-Type": "application/json",
    }


def recurly_mrr_and_subs() -> dict:
    """Returns {active_subs, mrr_cents, churned_this_month_cents}."""
    headers = _recurly_headers()
    if not headers:
        return {"active_subs": 0, "mrr_cents": 0, "churned_this_month_cents": 0}

    out = {"active_subs": 0, "mrr_cents": 0, "churned_this_month_cents": 0}

    # Active subs + MRR (estimate — use unit_amount * quantity for each active)
    try:
        url = f"{RECURLY_BASE}/sites/subdomain-seolocal/subscriptions"
        # the subdomain path uses the subdomain from SSM
        subdomain = get_ssm("RECURLY_SUBDOMAIN", required=False)
        if subdomain:
            # recurly API uses /sites/{subdomain}/subscriptions
            url = f"{RECURLY_BASE}/sites/{subdomain}/subscriptions"
        params = {"state": "active", "limit": 200}
        r = requests.get(url, headers=headers, params=params, timeout=20)
        if r.status_code < 400:
            data = r.json()
            subs = data.get("data", [])
            out["active_subs"] = len(subs)
            for s in subs:
                amt = int(s.get("unit_amount", 0) or 0) * int(
                    s.get("quantity", 1) or 1
                )
                out["mrr_cents"] += amt * 100 if amt and amt < 1000 else amt
    except Exception as e:
        log.warning("recurly_mrr active: %s", e)

    # Churned this month — subscriptions with state=canceled or expired in current month
    try:
        subdomain = get_ssm("RECURLY_SUBDOMAIN", required=False)
        base = f"{RECURLY_BASE}/sites/{subdomain}" if subdomain else ""
        if base:
            now = datetime.now(timezone.utc)
            start_of_month = now.replace(
                day=1, hour=0, minute=0, second=0, microsecond=0
            )
            url = f"{base}/subscriptions"
            params = {
                "state": "expired",
                "begin_time": start_of_month.isoformat(),
                "limit": 200,
            }
            r = requests.get(url, headers=headers, params=params, timeout=20)
            if r.status_code < 400:
                for s in r.json().get("data", []):
                    amt = int(s.get("unit_amount", 0) or 0) * int(
                        s.get("quantity", 1) or 1
                    )
                    out["churned_this_month_cents"] += (
                        amt * 100 if amt and amt < 1000 else amt
                    )
    except Exception as e:
        log.warning("recurly churn: %s", e)

    return out


# ---------------------------------------------------------------------------
# Gold Standards loader
# ---------------------------------------------------------------------------
def load_gold_standards() -> dict:
    bucket = get_ssm("S3_REPORTS_BUCKET", required=False)
    key = get_ssm("GOLD_STANDARDS_S3_KEY", required=False)
    if not bucket or not key:
        log.info(
            "gold_standards: S3_REPORTS_BUCKET or GOLD_STANDARDS_S3_KEY "
            "missing — using defaults"
        )
        return DEFAULT_GOLD_STANDARDS
    try:
        obj = _s3.get_object(Bucket=bucket, Key=key)
        parsed = json.loads(obj["Body"].read().decode("utf-8"))
        # shallow-merge defaults to avoid missing keys
        merged = {**DEFAULT_GOLD_STANDARDS, **parsed}
        return merged
    except Exception as e:
        log.warning("load_gold_standards failed: %s — using defaults", e)
        return DEFAULT_GOLD_STANDARDS


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------
def score_metric(value: float, thresholds: dict, higher_is_better: bool = True) -> str:
    """Return 'GREEN' | 'YELLOW' | 'RED'."""
    try:
        g = float(thresholds.get("green", 0))
        y = float(thresholds.get("yellow", 0))
        r = float(thresholds.get("red", 0))
    except Exception:
        return "GRAY"
    if higher_is_better:
        if value >= g:
            return "GREEN"
        if value >= y:
            return "YELLOW"
        return "RED"
    else:
        if value <= g:
            return "GREEN"
        if value <= y:
            return "YELLOW"
        return "RED"


def _emoji(color: str) -> str:
    return {"GREEN": "🟢", "YELLOW": "🟡", "RED": "🔴"}.get(color, "⚪")


# ---------------------------------------------------------------------------
# KPI collection
# ---------------------------------------------------------------------------
def collect_kpis() -> dict:
    """Pulls all KPIs and scores them. Returns dict."""
    kpis = {}
    gold = load_gold_standards()

    # Sales pipeline counts
    sales_pipeline = get_ssm("GHL_PIPELINE_ID", required=False)
    fulfillment_pipeline = get_ssm("GHL_PIPELINE_FULFILLMENT_ID", required=False)
    resurrection_pipeline = get_ssm("GHL_PIPELINE_RESURRECTION_ID", required=False)

    sales_stages = (
        ghl_pipeline_stages(sales_pipeline) if sales_pipeline else []
    )
    fulfillment_stages = (
        ghl_pipeline_stages(fulfillment_pipeline) if fulfillment_pipeline else []
    )
    resurrection_stages = (
        ghl_pipeline_stages(resurrection_pipeline) if resurrection_pipeline else []
    )

    sales_counts = {s.get("name", "?"): ghl_count_opportunities_in_stage(
        sales_pipeline, s.get("id")
    ) for s in sales_stages} if sales_stages else {}
    fulfillment_counts = {s.get("name", "?"): ghl_count_opportunities_in_stage(
        fulfillment_pipeline, s.get("id")
    ) for s in fulfillment_stages} if fulfillment_stages else {}
    resurrection_counts = {s.get("name", "?"): ghl_count_opportunities_in_stage(
        resurrection_pipeline, s.get("id")
    ) for s in resurrection_stages} if resurrection_stages else {}

    # Sales KPIs — show rate, close rate
    demo_booked = _sum_by_fragment(sales_counts, ["Demo Booked", "DEMO_BOOKED", "Demo Requested"])
    demo_completed = _sum_by_fragment(sales_counts, ["Demo Completed", "DEMO_COMPLETED", "Demo Attended"])
    closed_won = _sum_by_fragment(sales_counts, ["Signed Up", "Closed Won", "Active Client"])
    new_leads = _sum_by_fragment(sales_counts, ["New Lead", "NEW_LEAD"])
    audit_delivered = _sum_by_fragment(sales_counts, ["Audit Delivered", "AUDIT_DELIVERED"])

    show_rate = _safe_pct(demo_completed, demo_booked)
    close_rate = _safe_pct(closed_won, demo_completed)
    intake_completion = _safe_pct(audit_delivered, new_leads)

    kpis["show_rate_pct"] = {
        "value": show_rate,
        "score": score_metric(show_rate, gold["show_rate_pct"], True),
    }
    kpis["close_rate_pct"] = {
        "value": close_rate,
        "score": score_metric(close_rate, gold["close_rate_pct"], True),
    }
    kpis["intake_completion_pct"] = {
        "value": intake_completion,
        "score": score_metric(intake_completion, gold["intake_completion_pct"], True),
    }

    # Recurly
    r_data = recurly_mrr_and_subs()
    mrr = r_data["mrr_cents"] / 100.0
    churned = r_data["churned_this_month_cents"] / 100.0
    net_growth_pct = _safe_pct(mrr - churned, mrr) if mrr else 0.0

    kpis["active_subs"] = {"value": r_data["active_subs"], "score": "GRAY"}
    kpis["mrr"] = {"value": round(mrr, 2), "score": "GRAY"}
    kpis["churned_mrr_mo"] = {"value": round(churned, 2), "score": "GRAY"}
    kpis["mrr_growth_pct"] = {
        "value": round(net_growth_pct, 1),
        "score": score_metric(net_growth_pct, gold["mrr_growth_pct"], True),
    }

    # Retention
    churn_risk_count = ghl_contacts_by_tag("MOD:CHURN_RISK")
    kpis["churn_risk_contacts"] = {
        "value": churn_risk_count,
        "score": score_metric(
            churn_risk_count, gold["churn_risk_contacts"], higher_is_better=False
        ),
    }

    # Lead acquisition — leads this week
    leads_this_week = _leads_last_days(7)
    kpis["leads_this_week"] = {
        "value": leads_this_week,
        "score": score_metric(leads_this_week, gold["leads_this_week"], True),
    }

    return {
        "kpis": kpis,
        "sales_counts": sales_counts,
        "fulfillment_counts": fulfillment_counts,
        "resurrection_counts": resurrection_counts,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


def _sum_by_fragment(counts: dict, fragments: list) -> int:
    total = 0
    for name, v in counts.items():
        n_low = (name or "").lower()
        if any(f.lower() in n_low for f in fragments):
            total += int(v or 0)
    return total


def _safe_pct(num: float, den: float) -> float:
    if not den:
        return 0.0
    return round((num / den) * 100.0, 1)


def _leads_last_days(days: int) -> int:
    """Count GHL contacts created in last N days."""
    url = f"{GHL_BASE}/contacts/search"
    location_id = get_ssm("GHL_LOCATION_ID", required=False) or get_ssm(
        "LOCATION_ID", required=False
    )
    since = (datetime.now(timezone.utc) - timedelta(days=days)).strftime(
        "%Y-%m-%dT%H:%M:%S.000Z"
    )
    body = {
        "locationId": location_id,
        "pageLimit": 100,
        "filters": [
            {"field": "dateAdded", "operator": "gte", "value": since},
        ],
    }
    try:
        r = requests.post(url, headers=_ghl_headers(), json=body, timeout=20)
        if r.status_code >= 400:
            return 0
        data = r.json()
        total = data.get("total")
        if isinstance(total, int):
            return total
        return len(data.get("contacts", []))
    except Exception as e:
        log.warning("_leads_last_days error: %s", e)
        return 0


# ---------------------------------------------------------------------------
# Slack Block Kit builder
# ---------------------------------------------------------------------------
def build_report_blocks(report: dict) -> tuple:
    kpis = report["kpis"]
    any_red = any(v.get("score") == "RED" for v in kpis.values())
    overall = "🔴 RED" if any_red else (
        "🟡 YELLOW"
        if any(v.get("score") == "YELLOW" for v in kpis.values())
        else "🟢 GREEN"
    )

    lines = []
    lines.append(
        f"*Show Rate:* {_emoji(kpis['show_rate_pct']['score'])} "
        f"{kpis['show_rate_pct']['value']}%"
    )
    lines.append(
        f"*Close Rate:* {_emoji(kpis['close_rate_pct']['score'])} "
        f"{kpis['close_rate_pct']['value']}%"
    )
    lines.append(
        f"*Intake Completion:* {_emoji(kpis['intake_completion_pct']['score'])} "
        f"{kpis['intake_completion_pct']['value']}%"
    )
    lines.append(
        f"*Leads This Week:* {_emoji(kpis['leads_this_week']['score'])} "
        f"{kpis['leads_this_week']['value']}"
    )
    lines.append(
        f"*Active Subs:* {kpis['active_subs']['value']}  "
        f"*MRR:* ${kpis['mrr']['value']}  "
        f"*Churned (mo):* ${kpis['churned_mrr_mo']['value']}  "
        f"*Net Growth:* {_emoji(kpis['mrr_growth_pct']['score'])} "
        f"{kpis['mrr_growth_pct']['value']}%"
    )
    lines.append(
        f"*Churn-risk contacts:* {_emoji(kpis['churn_risk_contacts']['score'])} "
        f"{kpis['churn_risk_contacts']['value']}"
    )

    blocks = [
        {"type": "header", "text": {"type": "plain_text",
                                     "text": f"Pipeline Monitor — {overall}"}},
        {"type": "context", "elements": [
            {"type": "mrkdwn",
             "text": f"_Snapshot at {report['generated_at']}_"}
        ]},
        {"type": "section",
         "text": {"type": "mrkdwn", "text": "\n".join(lines)}},
    ]
    return blocks, any_red, overall


# ---------------------------------------------------------------------------
# Slack send
# ---------------------------------------------------------------------------
SLACK_API = "https://slack.com/api"


def slack_post(channel: str, blocks: list, text: str) -> bool:
    token = get_ssm("SLACK_BOT_TOKEN", required=False)
    if not token or not channel:
        log.info("slack_post: SLACK_BOT_TOKEN or channel missing — skipped")
        return False
    try:
        r = requests.post(
            f"{SLACK_API}/chat.postMessage",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json; charset=utf-8",
            },
            json={"channel": channel, "blocks": blocks, "text": text},
            timeout=20,
        )
        ok = r.status_code < 400 and r.json().get("ok")
        if not ok:
            log.error("slack_post failed: %s", r.text[:300])
        return bool(ok)
    except Exception as e:
        log.error("slack_post error: %s", e)
        return False


def slack_dm(user_id: str, text: str) -> bool:
    token = get_ssm("SLACK_BOT_TOKEN", required=False)
    if not token or not user_id:
        return False
    try:
        open_r = requests.post(
            f"{SLACK_API}/conversations.open",
            headers={"Authorization": f"Bearer {token}"},
            json={"users": user_id},
            timeout=20,
        )
        channel_id = (open_r.json().get("channel") or {}).get("id")
        if not channel_id:
            return False
        r = requests.post(
            f"{SLACK_API}/chat.postMessage",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json; charset=utf-8",
            },
            json={"channel": channel_id, "text": text},
            timeout=20,
        )
        return r.status_code < 400 and r.json().get("ok", False)
    except Exception as e:
        log.error("slack_dm error: %s", e)
        return False


# ---------------------------------------------------------------------------
# RDS snapshot (best-effort)
# ---------------------------------------------------------------------------
def _rds_conn():
    try:
        import psycopg2  # noqa: WPS433
    except Exception:
        return None
    try:
        return psycopg2.connect(
            host=get_ssm("RDS_HOST", required=False),
            dbname=get_ssm("RDS_DB_NAME", required=False),
            user=get_ssm("RDS_USERNAME", required=False),
            password=get_ssm("RDS_PASSWORD", required=False),
            connect_timeout=5,
        )
    except Exception as e:
        log.warning("RDS connect failed: %s", e)
        return None


def rds_write_snapshot(report: dict, overall: str) -> None:
    conn = _rds_conn()
    if not conn:
        return
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS pipeline_snapshots (
                  id SERIAL PRIMARY KEY,
                  snapshot_at TIMESTAMPTZ DEFAULT NOW(),
                  overall_status VARCHAR(16),
                  kpis_json JSONB
                )
                """
            )
            cur.execute(
                "INSERT INTO pipeline_snapshots (overall_status, kpis_json) "
                "VALUES (%s, %s::jsonb)",
                (overall, json.dumps(report)),
            )
    except Exception as e:
        log.warning("rds_write_snapshot failed: %s", e)
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Core handler
# ---------------------------------------------------------------------------
def handle_pipeline_check() -> dict:
    t0 = time.time()
    report = collect_kpis()
    blocks, any_red, overall = build_report_blocks(report)

    # Post to command center
    channel = get_ssm("SLACK_CHANNEL_COMMAND_CENTER", required=False)
    posted = slack_post(channel, blocks, f"Pipeline Monitor — {overall}")

    # DM Chuck on RED
    dm_sent = False
    if any_red:
        chuck = get_ssm("SLACK_CHUCK_USER_ID", required=False)
        if chuck:
            red_list = [
                k for k, v in report["kpis"].items() if v.get("score") == "RED"
            ]
            dm_sent = slack_dm(
                chuck,
                f":rotating_light: Pipeline RED — "
                f"{', '.join(red_list)}. Full report in "
                f"#c-suite-command-center.",
            )

    # RDS snapshot
    rds_write_snapshot(report, overall)

    return {
        "ok": True,
        "overall": overall,
        "any_red": any_red,
        "slack_posted": posted,
        "chuck_dm_sent": dm_sent,
        "kpi_count": len(report["kpis"]),
        "elapsed_ms": int((time.time() - t0) * 1000),
        "report": report,
    }


# ---------------------------------------------------------------------------
# AgentCore entrypoint
# ---------------------------------------------------------------------------
@app.entrypoint
def invoke(payload: dict) -> dict:
    """
    Payload shape:
      {"path": "/monitor/pipeline"}  -> full run
      {"path": "/ping"}              -> health check
    Cron callers typically send no 'path'; default is /monitor/pipeline.
    """
    try:
        path = (payload.get("path") or "/monitor/pipeline").rstrip("/")
        if path == "/monitor/pipeline":
            return handle_pipeline_check()
        if path in ("/ping", "/health"):
            return {"ok": True, "agent": "pipelineMonitor", "status": "healthy"}
        return {"ok": False, "error": f"unknown path: {path}"}
    except Exception as e:
        log.error("invoke failure: %s\n%s", e, traceback.format_exc())
        return {"ok": False, "error": str(e), "type": e.__class__.__name__}


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------

try:
    app.run()
except SystemExit:
    sys.exit(0)
except Exception as exc:
    log.error("fatal: %s", exc)
    sys.exit(1)
