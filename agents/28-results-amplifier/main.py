"""
Agent 28 â€” Results Amplifier
Phase 2 â€” Conversion & Retention Engine
AgentCore ID: resultsAmplifier

Purpose
-------
Turns client ranking wins into marketing assets. Runs monthly (1st of
month via EventBridge cron) AND on milestone webhooks. Detects Page 1
entries, Top 3 entries, and 90-Day Wins. Fires personalized review
capture SMS referencing the specific keyword + city. Scores incoming
testimonials across 5 dimensions (word count, specific result, 5-star,
competitor mention, location mention) and routes them:

  score >= 6  ->  MOD:CASE_STUDY_READY  + brief to #content-team
  score <  6  ->  SOCIAL_PROOF_ASSET    + added to testimonial pool

Posts milestone celebrations to #c-suite-command-center. DMs Chuck on
90-Day Wins. Chain-invokes Agent 22 (Video Engine) for milestone
videos when HEYGEN_MILESTONE_ACTIVE=true.

Endpoints
---------
POST /amplify/monthly      -> monthly cron scan (EventBridge 1st of month)
POST /amplify/milestone    -> single-milestone path (webhook)
POST /amplify/testimonial  -> testimonial scoring (webhook)
POST /ping                 -> health check

No gRPC. No hardcoded credentials. Secrets via AWS SSM Parameter Store.
Every external call is fallback-safe.
"""

import json
import logging
import os
import re
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
log = logging.getLogger("resultsAmplifier")

app = BedrockAgentCoreApp()

# ---------------------------------------------------------------------------
# AWS clients
# ---------------------------------------------------------------------------
REGION = os.environ.get("AWS_REGION", "us-east-1")
_ssm = boto3.client("ssm", region_name=REGION)
_s3 = boto3.client("s3", region_name=REGION)
_agentcore = boto3.client("bedrock-agentcore", region_name=REGION)

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
# Constants
# ---------------------------------------------------------------------------
MILESTONE_PAGE1 = "milestone_page1"      # rank <= 10, first time
MILESTONE_TOP3 = "milestone_top3"        # rank <= 3, first time
MILESTONE_90DAY = "milestone_90day"      # 90-day win (e.g. top-3 maintained)

CASE_STUDY_SCORE_THRESHOLD = 6

REVIEW_FOLLOWUP_DAYS = 2

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


def _location_id() -> str:
    return get_ssm("GHL_LOCATION_ID", required=False) or get_ssm(
        "LOCATION_ID", required=False
    )


def ghl_get_contact(contact_id: str) -> dict:
    url = f"{GHL_BASE}/contacts/{contact_id}"
    try:
        r = requests.get(url, headers=_ghl_headers(), timeout=20)
        if r.status_code == 404:
            return {}
        if r.status_code >= 400:
            log.warning("ghl_get_contact %s: %s", r.status_code, r.text[:300])
            return {}
        return r.json().get("contact", {}) or {}
    except Exception as e:
        log.warning("ghl_get_contact error: %s", e)
        return {}


def ghl_clients_by_tag(tag: str = "REL:CLIENT") -> list:
    """Returns list of contact dicts (id, firstName, city, business_name)."""
    url = f"{GHL_BASE}/contacts/search"
    out = []
    page = 1
    while page <= 20:
        body = {
            "locationId": _location_id(),
            "pageLimit": 100,
            "page": page,
            "filters": [
                {"field": "tags", "operator": "contains", "value": tag}
            ],
        }
        try:
            r = requests.post(url, headers=_ghl_headers(), json=body, timeout=25)
        except requests.RequestException as e:
            log.warning("ghl_clients_by_tag error: %s", e)
            break
        if r.status_code >= 400:
            log.warning(
                "ghl_clients_by_tag %s: %s", r.status_code, r.text[:300]
            )
            break
        data = r.json()
        batch = data.get("contacts", []) or []
        out.extend(batch)
        if len(batch) < 100:
            break
        page += 1
    return out


def ghl_send_sms(contact_id: str, message: str) -> bool:
    url = f"{GHL_BASE}/conversations/messages"
    body = {"type": "SMS", "contactId": contact_id, "message": message}
    try:
        r = requests.post(url, headers=_ghl_headers(), json=body, timeout=20)
        if r.status_code >= 400:
            log.error("ghl_send_sms %s: %s", r.status_code, r.text[:300])
            return False
        return True
    except Exception as e:
        log.error("ghl_send_sms error: %s", e)
        return False


def ghl_send_email(contact_id: str, subject: str, html: str) -> bool:
    url = f"{GHL_BASE}/conversations/messages"
    body = {
        "type": "Email",
        "contactId": contact_id,
        "subject": subject,
        "html": html,
    }
    try:
        r = requests.post(url, headers=_ghl_headers(), json=body, timeout=20)
        if r.status_code >= 400:
            log.error("ghl_send_email %s: %s", r.status_code, r.text[:300])
            return False
        return True
    except Exception as e:
        log.error("ghl_send_email error: %s", e)
        return False


def ghl_add_tag(contact_id: str, tag: str) -> bool:
    url = f"{GHL_BASE}/contacts/{contact_id}/tags"
    try:
        r = requests.post(
            url, headers=_ghl_headers(), json={"tags": [tag]}, timeout=20
        )
        return r.status_code < 400
    except Exception as e:
        log.warning("ghl_add_tag error: %s", e)
        return False


def ghl_update_custom_fields(contact_id: str, fields: dict) -> bool:
    if not fields:
        return True
    url = f"{GHL_BASE}/contacts/{contact_id}"
    body = {"customFields": [{"key": k, "value": v} for k, v in fields.items()]}
    try:
        r = requests.put(url, headers=_ghl_headers(), json=body, timeout=20)
        if r.status_code >= 400:
            log.warning(
                "ghl_update_custom_fields %s: %s",
                r.status_code,
                r.text[:300],
            )
            return False
        return True
    except Exception as e:
        log.warning("ghl_update_custom_fields error: %s", e)
        return False


# ---------------------------------------------------------------------------
# Slack helpers
# ---------------------------------------------------------------------------
SLACK_API = "https://slack.com/api"


def slack_post(channel: str, text: str, blocks: list | None = None) -> bool:
    token = get_ssm("SLACK_BOT_TOKEN", required=False)
    if not token or not channel:
        log.info("slack_post: token or channel missing â€” skipped")
        return False
    payload = {"channel": channel, "text": text}
    if blocks:
        payload["blocks"] = blocks
    try:
        r = requests.post(
            f"{SLACK_API}/chat.postMessage",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json; charset=utf-8",
            },
            json=payload,
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
            timeout=15,
        )
        channel = (open_r.json().get("channel") or {}).get("id")
        if not channel:
            return False
        r = requests.post(
            f"{SLACK_API}/chat.postMessage",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json; charset=utf-8",
            },
            json={"channel": channel, "text": text},
            timeout=15,
        )
        return r.status_code < 400 and r.json().get("ok", False)
    except Exception as e:
        log.error("slack_dm error: %s", e)
        return False


# ---------------------------------------------------------------------------
# RDS
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


def _ensure_tables(cur) -> None:
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS milestone_events (
          id SERIAL PRIMARY KEY,
          contact_id VARCHAR(100) NOT NULL,
          milestone_type VARCHAR(50) NOT NULL,
          keyword VARCHAR(200),
          city VARCHAR(200),
          position INT,
          detected_at TIMESTAMPTZ DEFAULT NOW()
        )
        """
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_milestone_contact_type "
        "ON milestone_events(contact_id, milestone_type, detected_at)"
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS testimonial_scores (
          id SERIAL PRIMARY KEY,
          contact_id VARCHAR(100) NOT NULL,
          score INT NOT NULL,
          pool VARCHAR(32) NOT NULL,
          payload JSONB,
          scored_at TIMESTAMPTZ DEFAULT NOW()
        )
        """
    )


def rds_latest_rank(contact_id: str, lookback_days: int = 120) -> list:
    """
    Pulls rank snapshots for a contact in the last N days, newest first.
    Returns list of dicts: {keyword, city, position, captured_at}.
    """
    conn = _rds_conn()
    if not conn:
        return []
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT keyword, city, position, captured_at
                FROM rank_snapshots
                WHERE contact_id = %s
                  AND captured_at > NOW() - INTERVAL %s
                ORDER BY captured_at DESC
                """,
                (contact_id, f"{lookback_days} days"),
            )
            return [
                {
                    "keyword": r[0],
                    "city": r[1],
                    "position": r[2],
                    "captured_at": r[3].isoformat() if r[3] else None,
                }
                for r in cur.fetchall()
            ]
    except Exception as e:
        log.warning("rds_latest_rank failed: %s", e)
        return []
    finally:
        try:
            conn.close()
        except Exception:
            pass


def rds_has_milestone(contact_id: str, milestone_type: str, keyword: str) -> bool:
    """True if this exact milestone was already fired for this keyword."""
    conn = _rds_conn()
    if not conn:
        return False
    try:
        with conn, conn.cursor() as cur:
            _ensure_tables(cur)
            cur.execute(
                "SELECT 1 FROM milestone_events "
                "WHERE contact_id = %s AND milestone_type = %s "
                "  AND COALESCE(keyword,'') = COALESCE(%s,'') "
                "LIMIT 1",
                (contact_id, milestone_type, keyword),
            )
            return cur.fetchone() is not None
    except Exception as e:
        log.warning("rds_has_milestone failed: %s", e)
        return False
    finally:
        try:
            conn.close()
        except Exception:
            pass


def rds_write_milestone(
    contact_id: str,
    milestone_type: str,
    keyword: str,
    city: str,
    position: int,
) -> None:
    conn = _rds_conn()
    if not conn:
        return
    try:
        with conn, conn.cursor() as cur:
            _ensure_tables(cur)
            cur.execute(
                "INSERT INTO milestone_events "
                "(contact_id, milestone_type, keyword, city, position) "
                "VALUES (%s, %s, %s, %s, %s)",
                (contact_id, milestone_type, keyword, city, int(position or 0)),
            )
    except Exception as e:
        log.warning("rds_write_milestone failed: %s", e)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def rds_write_testimonial(contact_id: str, score: int, pool: str, payload: dict) -> None:
    conn = _rds_conn()
    if not conn:
        return
    try:
        with conn, conn.cursor() as cur:
            _ensure_tables(cur)
            cur.execute(
                "INSERT INTO testimonial_scores "
                "(contact_id, score, pool, payload) VALUES (%s, %s, %s, %s::jsonb)",
                (contact_id, int(score), pool, json.dumps(payload)),
            )
    except Exception as e:
        log.warning("rds_write_testimonial failed: %s", e)
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Milestone detection
# ---------------------------------------------------------------------------
def detect_milestones(contact_id: str) -> list:
    """
    Returns list of milestone dicts:
      {type, keyword, city, position, captured_at}
    Each milestone is only returned if it has not already been recorded.
    """
    snaps = rds_latest_rank(contact_id, lookback_days=120)
    if not snaps:
        return []

    # group latest snapshot per keyword
    by_kw: dict = {}
    for s in snaps:
        kw = (s.get("keyword") or "").strip().lower()
        if kw and kw not in by_kw:
            by_kw[kw] = s

    milestones = []
    now = datetime.now(timezone.utc)

    for kw, latest in by_kw.items():
        pos = int(latest.get("position") or 999)
        city = latest.get("city") or ""

        # Page 1 entry (position 1..10, first time)
        if pos <= 10 and not rds_has_milestone(
            contact_id, MILESTONE_PAGE1, kw
        ):
            milestones.append(
                {
                    "type": MILESTONE_PAGE1,
                    "keyword": kw,
                    "city": city,
                    "position": pos,
                    "captured_at": latest.get("captured_at"),
                }
            )

        # Top 3 entry
        if pos <= 3 and not rds_has_milestone(
            contact_id, MILESTONE_TOP3, kw
        ):
            milestones.append(
                {
                    "type": MILESTONE_TOP3,
                    "keyword": kw,
                    "city": city,
                    "position": pos,
                    "captured_at": latest.get("captured_at"),
                }
            )

        # 90-day win â€” keyword currently top-3 and has been on page 1 for >=90 days
        if pos <= 3 and not rds_has_milestone(
            contact_id, MILESTONE_90DAY, kw
        ):
            kw_history = [
                s for s in snaps
                if (s.get("keyword") or "").lower() == kw
                and int(s.get("position") or 999) <= 10
            ]
            if kw_history:
                oldest_p1 = kw_history[-1]
                try:
                    oldest_dt = datetime.fromisoformat(
                        (oldest_p1["captured_at"] or "").replace("Z", "+00:00")
                    )
                    if (now - oldest_dt) >= timedelta(days=90):
                        milestones.append(
                            {
                                "type": MILESTONE_90DAY,
                                "keyword": kw,
                                "city": city,
                                "position": pos,
                                "captured_at": latest.get("captured_at"),
                            }
                        )
                except Exception:
                    pass

    return milestones


# ---------------------------------------------------------------------------
# Milestone action (review capture + Slack + video)
# ---------------------------------------------------------------------------
def _pretty_milestone(m_type: str) -> str:
    return {
        MILESTONE_PAGE1: "Page 1",
        MILESTONE_TOP3: "Top 3",
        MILESTONE_90DAY: "90-Day Win",
    }.get(m_type, m_type)


def _review_link(contact: dict) -> str:
    """Prefer contact custom field GOOGLE_REVIEW_LINK; else generic fallback."""
    for cf in contact.get("customFields", []) or []:
        key = (cf.get("key") or cf.get("fieldKey") or "").upper()
        if key in ("GOOGLE_REVIEW_LINK", "REVIEW_LINK"):
            v = cf.get("value")
            if v:
                return str(v)
    return "https://g.page/r/review"


def fire_milestone(contact_id: str, milestone: dict) -> dict:
    contact = ghl_get_contact(contact_id)
    if not contact:
        return {"ok": False, "error": "contact_not_found"}

    first = (
        contact.get("firstName")
        or contact.get("first_name")
        or "there"
    ).strip()
    business = (
        contact.get("companyName")
        or contact.get("company_name")
        or contact.get("businessName")
        or "your business"
    )
    keyword = milestone.get("keyword") or ""
    city = milestone.get("city") or "your city"
    position = int(milestone.get("position") or 0)
    m_type = milestone.get("type")
    pretty = _pretty_milestone(m_type)
    review_link = _review_link(contact)

    # 1. stamp custom fields
    ghl_update_custom_fields(
        contact_id,
        {
            "RANKING_MILESTONE_HIT": "true",
            "MILESTONE_TYPE": m_type,
            "MILESTONE_KEYWORD": keyword,
            "MILESTONE_CITY": city,
            "MILESTONE_POSITION": str(position),
            "MILESTONE_DATE": datetime.now(timezone.utc).isoformat(),
        },
    )

    # 2. review capture SMS
    sms_text = (
        f"{first}, huge milestone â€” \"{keyword}\" just hit position "
        f"#{position} in {city}! Could you spare 60 seconds to drop us a "
        f"quick Google review? {review_link}"
    )
    sms_ok = ghl_send_sms(contact_id, sms_text)

    # 3. tag for follow-up / review request
    ghl_add_tag(contact_id, "MOD:REVIEW_REQUESTED")

    # 4. Slack command center post
    channel = get_ssm("SLACK_CHANNEL_COMMAND_CENTER", required=False)
    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"ðŸ† MILESTONE â€” {pretty}",
            },
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"*Client:* {first} â€” {business}\n"
                    f"*Keyword:* `{keyword}`\n"
                    f"*City:* {city}\n"
                    f"*Position:* #{position}"
                ),
            },
        },
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": (
                        f"_Review SMS fired: {'âœ“' if sms_ok else 'âœ—'}_"
                    ),
                }
            ],
        },
    ]
    slack_ok = slack_post(
        channel, f"MILESTONE â€” {pretty} â€” {business}", blocks
    )

    # 5. DM Chuck on 90-day win
    dm_ok = False
    if m_type == MILESTONE_90DAY:
        chuck = get_ssm("SLACK_CHUCK_USER_ID", required=False)
        if chuck:
            dm_ok = slack_dm(
                chuck,
                f"ðŸ† 90-DAY WIN â€” {business} â€” \"{keyword}\" holding "
                f"#{position} in {city}. This is a case study candidate.",
            )

    # 6. fire video if milestone video is live
    video_invoked = False
    if str(
        get_ssm("HEYGEN_MILESTONE_ACTIVE", required=False, default="false")
    ).strip().lower() in ("true", "1", "yes", "on"):
        video_invoked = invoke_video_engine(
            video_type=m_type,
            contact=contact,
            keyword=keyword,
            city=city,
            position=position,
        )

    # 7. record milestone
    rds_write_milestone(contact_id, m_type, keyword, city, position)

    return {
        "ok": True,
        "milestone_type": m_type,
        "keyword": keyword,
        "city": city,
        "position": position,
        "sms_sent": sms_ok,
        "slack_posted": slack_ok,
        "chuck_dm_sent": dm_ok,
        "video_invoked": video_invoked,
    }


# ---------------------------------------------------------------------------
# Chain-invoke Agent 22 (Video Engine)
# ---------------------------------------------------------------------------
def invoke_video_engine(
    video_type: str,
    contact: dict,
    keyword: str,
    city: str,
    position: int,
) -> bool:
    """Best-effort invoke of Agent 22 via its runtime ARN in SSM."""
    arn = get_ssm("VIDEO_ENGINE_RUNTIME_ARN", required=False)
    if not arn:
        log.info(
            "video_engine not wired: VIDEO_ENGINE_RUNTIME_ARN not set"
        )
        return False
    payload = {
        "path": "/video/generate",
        "video_type": video_type,
        "contact_id": contact.get("id") or contact.get("contactId"),
        "first_name": contact.get("firstName") or "there",
        "city": city,
        "business_name": (
            contact.get("companyName")
            or contact.get("businessName")
            or "your business"
        ),
        "primary_gap": "ranking",
        "ranking_keywords": [keyword] if keyword else [],
        "milestone_type": video_type,
        "delivery_channels": ["sms", "email"],
    }
    try:
        _agentcore.invoke_agent_runtime(
            agentRuntimeArn=arn,
            qualifier="DEFAULT",
            payload=json.dumps(payload).encode("utf-8"),
            contentType="application/json",
        )
        return True
    except Exception as e:
        log.warning("invoke_video_engine failed: %s", e)
        return False


# ---------------------------------------------------------------------------
# Testimonial scoring
# ---------------------------------------------------------------------------
_COMPETITOR_HINTS = {
    "better than",
    "switched from",
    "used to use",
    "compared to",
    "vs ",
    "instead of",
}


def score_testimonial(
    text: str,
    stars: int = 0,
    city_hint: str = "",
    business_hint: str = "",
) -> tuple:
    """
    Returns (score, breakdown_dict).
    Dimensions:
      +2 word count >= 50
      +3 specific result (contains number or %)
      +2 five-star (stars == 5)
      +1 mentions a competitor / comparison
      +1 mentions location
    """
    t = (text or "").strip()
    low = t.lower()
    breakdown = {}

    words = len(re.findall(r"\b\w+\b", t))
    breakdown["word_count_ge_50"] = 2 if words >= 50 else 0

    has_number = bool(re.search(r"\b\d+([.,]\d+)?\s*%?\b", t))
    breakdown["specific_result"] = 3 if has_number else 0

    breakdown["five_star"] = 2 if int(stars or 0) == 5 else 0

    breakdown["competitor_mention"] = (
        1 if any(hint in low for hint in _COMPETITOR_HINTS) else 0
    )

    loc_hit = False
    if city_hint and city_hint.lower() in low:
        loc_hit = True
    else:
        if re.search(
            r"\b(city|town|area|downtown|near me)\b", low
        ):
            loc_hit = True
    breakdown["location_mention"] = 1 if loc_hit else 0

    score = sum(breakdown.values())
    return score, breakdown


# ---------------------------------------------------------------------------
# Case study brief (S3 + Slack content-team)
# ---------------------------------------------------------------------------
def _case_study_key(contact_id: str) -> str:
    prefix = get_ssm("S3_CASE_STUDIES_PREFIX", required=False, default="case_studies/")
    if not prefix.endswith("/"):
        prefix = prefix + "/"
    ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    return f"{prefix}{contact_id}-{ts}.json"


def write_case_study_brief(contact_id: str, contact: dict, testimonial: dict) -> str:
    bucket = get_ssm("S3_REPORTS_BUCKET", required=False)
    if not bucket:
        return ""
    key = _case_study_key(contact_id)
    body = {
        "contact_id": contact_id,
        "first_name": contact.get("firstName") or "",
        "business_name": (
            contact.get("companyName") or contact.get("businessName") or ""
        ),
        "city": contact.get("city") or "",
        "testimonial": testimonial,
        "written_at": datetime.now(timezone.utc).isoformat(),
    }
    try:
        _s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(body, indent=2).encode("utf-8"),
            ContentType="application/json",
        )
        return f"s3://{bucket}/{key}"
    except Exception as e:
        log.warning("write_case_study_brief failed: %s", e)
        return ""


def post_case_study_brief(contact: dict, testimonial: dict, s3_uri: str) -> bool:
    channel = get_ssm("SLACK_CHANNEL_CONTENT_TEAM", required=False)
    if not channel:
        return False
    business = (
        contact.get("companyName") or contact.get("businessName") or "client"
    )
    first = contact.get("firstName") or ""
    text = testimonial.get("text") or ""
    snippet = (text[:260] + "â€¦") if len(text) > 260 else text
    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"ðŸ“ Case Study Candidate â€” {business}",
            },
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"*Client:* {first} â€” {business}\n"
                    f"*Testimonial:*\n>{snippet}"
                ),
            },
        },
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"Brief: `{s3_uri}`" if s3_uri else "_brief not stored_",
                }
            ],
        },
    ]
    return slack_post(
        channel, f"Case Study Candidate â€” {business}", blocks
    )


# ---------------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------------
def handle_monthly(payload: dict) -> dict:
    t0 = time.time()
    clients = ghl_clients_by_tag("REL:CLIENT")
    log.info("monthly scan: %d clients", len(clients))

    milestones_fired = 0
    per_client = []

    for c in clients:
        cid = c.get("id") or c.get("contactId")
        if not cid:
            continue
        detected = detect_milestones(cid)
        for m in detected:
            res = fire_milestone(cid, m)
            if res.get("ok"):
                milestones_fired += 1
        per_client.append({"contact_id": cid, "milestones": len(detected)})

    # Command center summary
    channel = get_ssm("SLACK_CHANNEL_COMMAND_CENTER", required=False)
    slack_post(
        channel,
        f"ðŸ“… Monthly Results Amplifier: scanned {len(clients)} clients, "
        f"fired {milestones_fired} milestones",
    )

    return {
        "ok": True,
        "clients_scanned": len(clients),
        "milestones_fired": milestones_fired,
        "per_client": per_client,
        "elapsed_ms": int((time.time() - t0) * 1000),
    }


def handle_milestone(payload: dict) -> dict:
    t0 = time.time()
    contact_id = (payload.get("contact_id") or "").strip()
    if not contact_id:
        return {"ok": False, "error": "contact_id required"}

    # caller may provide the milestone directly; else we detect
    if payload.get("milestone_type"):
        m = {
            "type": payload["milestone_type"],
            "keyword": payload.get("keyword") or "",
            "city": payload.get("city") or "",
            "position": int(payload.get("position") or 0),
        }
        if rds_has_milestone(contact_id, m["type"], m["keyword"]):
            return {"ok": True, "status": "already_fired", "milestone_type": m["type"]}
        res = fire_milestone(contact_id, m)
        res["elapsed_ms"] = int((time.time() - t0) * 1000)
        return res

    detected = detect_milestones(contact_id)
    if not detected:
        return {
            "ok": True,
            "status": "no_new_milestones",
            "contact_id": contact_id,
            "elapsed_ms": int((time.time() - t0) * 1000),
        }
    results = [fire_milestone(contact_id, m) for m in detected]
    return {
        "ok": True,
        "milestones_fired": sum(1 for r in results if r.get("ok")),
        "results": results,
        "elapsed_ms": int((time.time() - t0) * 1000),
    }


def handle_testimonial(payload: dict) -> dict:
    t0 = time.time()
    contact_id = (payload.get("contact_id") or "").strip()
    text = payload.get("text") or payload.get("review_text") or ""
    stars = int(payload.get("stars") or payload.get("rating") or 0)

    if not contact_id or not text:
        return {"ok": False, "error": "contact_id and text required"}

    contact = ghl_get_contact(contact_id)
    city_hint = (contact.get("city") or "").strip() if contact else ""
    business_hint = (
        contact.get("companyName") or contact.get("businessName") or ""
    ) if contact else ""

    score, breakdown = score_testimonial(
        text, stars=stars, city_hint=city_hint, business_hint=business_hint
    )

    pool = (
        "CASE_STUDY_READY"
        if score >= CASE_STUDY_SCORE_THRESHOLD
        else "SOCIAL_PROOF_ASSET"
    )
    tag = (
        "MOD:CASE_STUDY_READY"
        if pool == "CASE_STUDY_READY"
        else "SOCIAL_PROOF_ASSET"
    )

    ghl_add_tag(contact_id, tag)
    ghl_update_custom_fields(contact_id, {"TESTIMONIAL_SCORE": str(score)})

    s3_uri = ""
    content_posted = False
    if pool == "CASE_STUDY_READY":
        testimonial_payload = {
            "text": text,
            "stars": stars,
            "score": score,
            "breakdown": breakdown,
        }
        s3_uri = write_case_study_brief(
            contact_id, contact or {}, testimonial_payload
        )
        content_posted = post_case_study_brief(
            contact or {}, testimonial_payload, s3_uri
        )

    rds_write_testimonial(
        contact_id,
        score,
        pool,
        {"text": text, "stars": stars, "breakdown": breakdown},
    )

    return {
        "ok": True,
        "contact_id": contact_id,
        "score": score,
        "breakdown": breakdown,
        "pool": pool,
        "tag_applied": tag,
        "s3_uri": s3_uri,
        "content_team_notified": content_posted,
        "elapsed_ms": int((time.time() - t0) * 1000),
    }


# ---------------------------------------------------------------------------
# AgentCore entrypoint
# ---------------------------------------------------------------------------
@app.entrypoint
def invoke(payload: dict) -> dict:
    """
    Payload shape:
      {"path": "/amplify/monthly"}
      {"path": "/amplify/milestone", "contact_id": "...", "milestone_type": "..."}
      {"path": "/amplify/testimonial", "contact_id": "...", "text": "...", "stars": 5}
      {"path": "/ping"}
    """
    try:
        path = (payload.get("path") or "/amplify/monthly").rstrip("/")
        if path == "/amplify/monthly":
            return handle_monthly(payload)
        if path == "/amplify/milestone":
            return handle_milestone(payload)
        if path == "/amplify/testimonial":
            return handle_testimonial(payload)
        if path in ("/ping", "/health"):
            return {"ok": True, "agent": "resultsAmplifier", "status": "healthy"}
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
