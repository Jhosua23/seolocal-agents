"""
Agent 22 â€” Video Engine
Phase 2 â€” Conversion & Retention Engine
AgentCore ID: videoEngine

Purpose
-------
Central AI video dispatcher for the whole Marketing Engine. Any caller
agent invokes /video/generate with a video_type + contact fields. This
agent:

  1. validates video_type is in the approved whitelist
  2. checks the per-type feature flag (HEYGEN_<TYPE>_ACTIVE)
  3. applies a 7-day dedupe guard (RDS video_deliveries)
  4. loads heygen_config.json from S3 to pick template + variables
  5. calls HeyGen /v2/video/generate
  6. polls HeyGen until the video is ready (or 5-minute timeout)
  7. delivers the generated video via GHL SMS and/or email
  8. stamps GHL custom fields + tag, writes RDS delivery row

Endpoints
---------
POST /video/generate  -> generate + deliver one video
POST /ping            -> health check

No gRPC. No hardcoded credentials. Secrets via AWS SSM Parameter Store.
Every external call is fallback-safe: the agent never 500s because of a
missing optional param or an upstream hiccup â€” it returns a structured
error dict instead.
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
log = logging.getLogger("videoEngine")

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
# Whitelisted video types + their feature flag param + SLA / length
# ---------------------------------------------------------------------------
VIDEO_TYPES = {
    "speed_to_lead": {
        "flag": "HEYGEN_SPEED_TO_LEAD_ACTIVE",
        "sla_seconds": 300,
    },
    "hot_lead": {
        "flag": "HEYGEN_HOT_LEAD_ACTIVE",
        "sla_seconds": 300,
    },
    "pre_demo": {
        "flag": "HEYGEN_PRE_DEMO_ACTIVE",
        "sla_seconds": 86400,
    },
    "closed_won": {
        "flag": "HEYGEN_CLOSED_WON_ACTIVE",
        "sla_seconds": 600,
    },
    "onboarding_welcome": {
        "flag": "HEYGEN_ONBOARDING_ACTIVE",
        "sla_seconds": 7200,
    },
    "monthly_results": {
        "flag": "HEYGEN_MONTHLY_RESULTS_ACTIVE",
        "sla_seconds": 86400,
    },
    "hand_raise": {
        "flag": "HEYGEN_HAND_RAISE_ACTIVE",
        "sla_seconds": 300,
    },
    "milestone_page1": {
        "flag": "HEYGEN_MILESTONE_ACTIVE",
        "sla_seconds": 3600,
    },
    "milestone_top3": {
        "flag": "HEYGEN_MILESTONE_ACTIVE",
        "sla_seconds": 3600,
    },
    "milestone_90day": {
        "flag": "HEYGEN_MILESTONE_ACTIVE",
        "sla_seconds": 14400,
    },
}

DEDUPE_WINDOW_DAYS = 7
HEYGEN_POLL_INTERVAL_SECONDS = 15
HEYGEN_POLL_TIMEOUT_SECONDS = 300

# ---------------------------------------------------------------------------
# Built-in default heygen_config.json â€” used only if S3 unreachable or
# the key is not set in SSM. Lets the agent deploy and self-smoke-test
# without external config.
# ---------------------------------------------------------------------------
DEFAULT_HEYGEN_CONFIG = {
    "speed_to_lead": {
        "template_id": "",
        "script": (
            "Hi {first_name}, this is Chuck from SEO Local. Thanks for "
            "requesting your audit for {business_name} in {city}. I'll have "
            "your report back within the hour. Talk soon."
        ),
    },
    "hot_lead": {
        "template_id": "",
        "script": (
            "Hey {first_name} â€” Chuck here. I saw you just engaged with our "
            "audit for {business_name}. I have a quick insight about "
            "{primary_gap} in {city} that I think you'll want. Let's hop on "
            "a quick call."
        ),
    },
    "pre_demo": {
        "template_id": "",
        "script": (
            "Hi {first_name}, looking forward to our demo tomorrow. I'll walk "
            "you through exactly where {business_name} stands in {city} and "
            "what the top-3 path looks like. See you then."
        ),
    },
    "closed_won": {
        "template_id": "",
        "script": (
            "Welcome aboard, {first_name}! Thrilled to have {business_name} "
            "on the team. Our onboarding crew will be in touch within the "
            "next hour. Let's win {city}."
        ),
    },
    "onboarding_welcome": {
        "template_id": "",
        "script": (
            "{first_name}, quick hello from the fulfillment team. We'll "
            "kick off {business_name}'s campaign this week â€” here's what "
            "to expect in your first 30 days."
        ),
    },
    "monthly_results": {
        "template_id": "",
        "script": (
            "{first_name}, your monthly SEO Local report is ready. Here are "
            "the ranking moves for {business_name} in {city} this month."
        ),
    },
    "hand_raise": {
        "template_id": "",
        "script": (
            "Hey {first_name} â€” I saw you clicked through. Great timing. "
            "Let me grab 15 minutes to walk you through where "
            "{business_name} stands and what your next step would look like."
        ),
    },
    "milestone_page1": {
        "template_id": "",
        "script": (
            "{first_name}, huge milestone â€” {business_name} just hit Page 1 "
            "in {city}. This is what the climb to the Map Pack looks like."
        ),
    },
    "milestone_top3": {
        "template_id": "",
        "script": (
            "{first_name}, you did it â€” {business_name} is now in the Top 3 "
            "for {city}. This is exactly what we were working toward. "
            "Congrats."
        ),
    },
    "milestone_90day": {
        "template_id": "",
        "script": (
            "{first_name}, 90 days in and {business_name} is dominating "
            "{city}. Here's the full breakdown of what we achieved together."
        ),
    },
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
    """
    Update contact custom fields. Accepts {field_name: value} dict.
    GHL v2 expects customFields array of {id or key, value}. We send as 'key'.
    """
    if not fields:
        return True
    url = f"{GHL_BASE}/contacts/{contact_id}"
    custom_fields = [{"key": k, "value": v} for k, v in fields.items()]
    body = {"customFields": custom_fields}
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
# Slack alert (used only when HeyGen times out â€” non-blocking)
# ---------------------------------------------------------------------------
def slack_alert_chuck(text: str) -> None:
    token = get_ssm("SLACK_BOT_TOKEN", required=False)
    chuck_id = get_ssm("SLACK_CHUCK_USER_ID", required=False)
    if not token or not chuck_id:
        return
    try:
        open_r = requests.post(
            "https://slack.com/api/conversations.open",
            headers={"Authorization": f"Bearer {token}"},
            json={"users": chuck_id},
            timeout=15,
        )
        channel = (open_r.json().get("channel") or {}).get("id")
        if not channel:
            return
        requests.post(
            "https://slack.com/api/chat.postMessage",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json; charset=utf-8",
            },
            json={"channel": channel, "text": text},
            timeout=15,
        )
    except Exception as e:
        log.warning("slack_alert_chuck error: %s", e)


# ---------------------------------------------------------------------------
# HeyGen helpers
# ---------------------------------------------------------------------------
HEYGEN_BASE = "https://api.heygen.com"


def _heygen_headers() -> dict:
    return {
        "X-Api-Key": get_ssm("HEYGEN_API_KEY"),
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def load_heygen_config() -> dict:
    """Pull heygen_config.json from S3. Fall back to built-in defaults."""
    bucket = get_ssm("S3_REPORTS_BUCKET", required=False)
    key = get_ssm("HEYGEN_CONFIG_S3_KEY", required=False)
    if not bucket or not key:
        log.info(
            "heygen_config: S3 bucket or key missing â€” using built-in defaults"
        )
        return DEFAULT_HEYGEN_CONFIG
    try:
        obj = _s3.get_object(Bucket=bucket, Key=key)
        parsed = json.loads(obj["Body"].read().decode("utf-8"))
        merged = {**DEFAULT_HEYGEN_CONFIG, **parsed}
        return merged
    except Exception as e:
        log.warning("load_heygen_config failed: %s â€” using defaults", e)
        return DEFAULT_HEYGEN_CONFIG


def build_script(template: str, variables: dict) -> str:
    """Safe .format_map that swallows missing keys."""
    class _SafeDict(dict):
        def __missing__(self, k):
            return ""

    try:
        return template.format_map(_SafeDict(variables))
    except Exception as e:
        log.warning("build_script fallback (format error %s)", e)
        return template


def heygen_generate(
    script: str, template_id: str
) -> str:
    """
    Kick off a HeyGen generation. Returns heygen video_id.
    Works in 2 modes:
      - If template_id is set â†’ /v2/template/{id}/generate
      - Otherwise â†’ /v2/video/generate with avatar_id + voice_id
    Raises RuntimeError on API failure.
    """
    avatar = get_ssm("HEYGEN_AVATAR_ID", required=False)
    voice = get_ssm("HEYGEN_VOICE_ID", required=False)

    if template_id:
        url = f"{HEYGEN_BASE}/v2/template/{template_id}/generate"
        body = {"variables": {"script": {"name": "script", "type": "text",
                                          "properties": {"content": script}}}}
    else:
        if not avatar or not voice:
            raise RuntimeError(
                "HeyGen template_id missing AND HEYGEN_AVATAR_ID / "
                "HEYGEN_VOICE_ID not set â€” cannot generate video"
            )
        url = f"{HEYGEN_BASE}/v2/video/generate"
        body = {
            "video_inputs": [
                {
                    "character": {
                        "type": "avatar",
                        "avatar_id": avatar,
                        "avatar_style": "normal",
                    },
                    "voice": {
                        "type": "text",
                        "input_text": script,
                        "voice_id": voice,
                    },
                    "background": {"type": "color", "value": "#f6f6fc"},
                }
            ],
            "dimension": {"width": 1280, "height": 720},
        }

    r = requests.post(url, headers=_heygen_headers(), json=body, timeout=30)
    if r.status_code >= 400:
        raise RuntimeError(
            f"HeyGen generate failed {r.status_code}: {r.text[:300]}"
        )
    data = r.json()
    video_id = (data.get("data") or {}).get("video_id") or data.get("video_id")
    if not video_id:
        raise RuntimeError(f"HeyGen generate returned no video_id: {data}")
    return video_id


def heygen_poll(video_id: str) -> str:
    """
    Poll HeyGen until status=completed or timeout. Returns video_url.
    Raises RuntimeError on failure / timeout.
    """
    url = f"{HEYGEN_BASE}/v1/video_status.get"
    params = {"video_id": video_id}
    deadline = time.time() + HEYGEN_POLL_TIMEOUT_SECONDS
    last_status = None
    while time.time() < deadline:
        try:
            r = requests.get(
                url, headers=_heygen_headers(), params=params, timeout=20
            )
            if r.status_code >= 400:
                log.warning(
                    "heygen_poll %s: %s", r.status_code, r.text[:300]
                )
            else:
                data = r.json().get("data") or {}
                status = data.get("status")
                last_status = status
                if status == "completed":
                    vurl = data.get("video_url")
                    if vurl:
                        return vurl
                    raise RuntimeError("HeyGen completed but no video_url")
                if status in ("failed", "error"):
                    raise RuntimeError(
                        f"HeyGen generation failed: {data}"
                    )
        except requests.RequestException as e:
            log.warning("heygen_poll network error: %s", e)
        time.sleep(HEYGEN_POLL_INTERVAL_SECONDS)

    raise RuntimeError(
        f"HeyGen poll timeout after {HEYGEN_POLL_TIMEOUT_SECONDS}s, "
        f"last_status={last_status}"
    )


# ---------------------------------------------------------------------------
# RDS â€” dedupe + write delivery
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


def _ensure_table(cur) -> None:
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS video_deliveries (
          id SERIAL PRIMARY KEY,
          contact_id VARCHAR(100) NOT NULL,
          video_type VARCHAR(50) NOT NULL,
          heygen_video_id VARCHAR(100),
          video_url TEXT,
          sms_delivered BOOLEAN DEFAULT FALSE,
          email_delivered BOOLEAN DEFAULT FALSE,
          generation_ms INT,
          delivered_ts TIMESTAMPTZ,
          created_at TIMESTAMPTZ DEFAULT NOW()
        )
        """
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_video_deliveries_contact_type "
        "ON video_deliveries(contact_id, video_type, created_at)"
    )


def rds_check_dedupe(contact_id: str, video_type: str) -> bool:
    """True if an identical delivery exists within the dedupe window."""
    conn = _rds_conn()
    if not conn:
        return False
    try:
        with conn, conn.cursor() as cur:
            _ensure_table(cur)
            cur.execute(
                "SELECT 1 FROM video_deliveries "
                "WHERE contact_id = %s AND video_type = %s "
                "  AND created_at > NOW() - INTERVAL %s "
                "LIMIT 1",
                (contact_id, video_type, f"{DEDUPE_WINDOW_DAYS} days"),
            )
            return cur.fetchone() is not None
    except Exception as e:
        log.warning("rds_check_dedupe failed: %s", e)
        return False
    finally:
        try:
            conn.close()
        except Exception:
            pass


def rds_write_delivery(
    contact_id: str,
    video_type: str,
    heygen_video_id: str,
    video_url: str,
    sms_ok: bool,
    email_ok: bool,
    generation_ms: int,
) -> None:
    conn = _rds_conn()
    if not conn:
        return
    try:
        with conn, conn.cursor() as cur:
            _ensure_table(cur)
            cur.execute(
                """
                INSERT INTO video_deliveries
                  (contact_id, video_type, heygen_video_id, video_url,
                   sms_delivered, email_delivered, generation_ms,
                   delivered_ts)
                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                """,
                (
                    contact_id,
                    video_type,
                    heygen_video_id,
                    video_url,
                    bool(sms_ok),
                    bool(email_ok),
                    int(generation_ms or 0),
                ),
            )
    except Exception as e:
        log.warning("rds_write_delivery failed: %s", e)
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Core handler
# ---------------------------------------------------------------------------
def _feature_flag_on(video_type: str) -> bool:
    flag_name = VIDEO_TYPES[video_type]["flag"]
    val = get_ssm(flag_name, required=False, default="false")
    return str(val).strip().lower() in ("true", "1", "yes", "on")


def _hash_contact(cid: str) -> str:
    import hashlib

    return hashlib.sha256(cid.encode("utf-8")).hexdigest()[:12]


def handle_generate(payload: dict) -> dict:
    t0 = time.time()

    # --- 1. validate --------------------------------------------------------
    video_type = (payload.get("video_type") or "").strip().lower()
    contact_id = (payload.get("contact_id") or "").strip()
    channels = payload.get("delivery_channels") or ["sms", "email"]

    if video_type not in VIDEO_TYPES:
        return {
            "ok": False,
            "error": f"unknown video_type: {video_type}",
            "allowed": list(VIDEO_TYPES.keys()),
        }
    if not contact_id:
        return {"ok": False, "error": "contact_id required"}
    if not isinstance(channels, list) or not channels:
        return {"ok": False, "error": "delivery_channels must be non-empty list"}

    # --- 2. feature flag ----------------------------------------------------
    if not _feature_flag_on(video_type):
        log.info(
            "video_type %s inactive (flag %s != true) â€” exiting clean",
            video_type,
            VIDEO_TYPES[video_type]["flag"],
        )
        return {
            "ok": True,
            "status": "inactive",
            "video_type": video_type,
            "reason": "feature flag off",
        }

    # --- 3. dedupe ----------------------------------------------------------
    if rds_check_dedupe(contact_id, video_type):
        log.info(
            "dedupe hit: %s/%s within %dd",
            _hash_contact(contact_id),
            video_type,
            DEDUPE_WINDOW_DAYS,
        )
        return {
            "ok": True,
            "status": "deduped",
            "video_type": video_type,
            "contact_hash": _hash_contact(contact_id),
        }

    # --- 4. template + script ----------------------------------------------
    config = load_heygen_config()
    tpl_cfg = config.get(video_type) or {}
    template_id = (tpl_cfg.get("template_id") or "").strip()
    script_template = tpl_cfg.get("script") or ""

    variables = {
        "first_name": payload.get("first_name") or "there",
        "city": payload.get("city") or "your city",
        "business_name": payload.get("business_name") or "your business",
        "primary_gap": payload.get("primary_gap") or "local search",
        "ranking_keywords": ", ".join(payload.get("ranking_keywords") or []),
        "milestone_type": payload.get("milestone_type") or "",
    }
    script = build_script(script_template, variables)

    if not script.strip():
        return {
            "ok": False,
            "error": "generated script is empty â€” check heygen_config.json",
            "video_type": video_type,
        }

    # --- 5. HeyGen generate + poll -----------------------------------------
    try:
        heygen_video_id = heygen_generate(script, template_id)
        video_url = heygen_poll(heygen_video_id)
    except RuntimeError as e:
        log.error("HeyGen failure for %s: %s", video_type, e)
        slack_alert_chuck(
            f":warning: Video Engine â€” HeyGen failed for "
            f"video_type={video_type}, contact_hash="
            f"{_hash_contact(contact_id)}. Error: {str(e)[:200]}"
        )
        return {
            "ok": False,
            "error": "heygen_failure",
            "detail": str(e)[:300],
            "video_type": video_type,
        }

    generation_ms = int((time.time() - t0) * 1000)

    # --- 6. delivery --------------------------------------------------------
    sms_ok = False
    email_ok = False
    first = variables["first_name"]

    if "sms" in channels:
        sms_text = (
            f"Hi {first} â€” quick personal video for you: {video_url}"
        )
        sms_ok = ghl_send_sms(contact_id, sms_text)

    if "email" in channels:
        subj_map = {
            "speed_to_lead": "Your audit â€” quick hello",
            "hot_lead": "A quick 30-second video for you",
            "pre_demo": "Looking forward to our call",
            "closed_won": "Welcome aboard",
            "onboarding_welcome": "Quick hello from the team",
            "monthly_results": "Your monthly SEO Local report",
            "hand_raise": "Saw you clicked â€” here's a quick video",
            "milestone_page1": "Big milestone â€” Page 1",
            "milestone_top3": "You did it â€” Top 3",
            "milestone_90day": "90-Day Win â€” recap",
        }
        subject = subj_map.get(video_type, "A quick video from SEO Local")
        html = (
            f"<html><body style='font-family:Arial,sans-serif;"
            f"line-height:1.5;color:#222;'>"
            f"<p>Hi {first},</p>"
            f"<p>Quick personal video for you:</p>"
            f"<p><a href='{video_url}'>"
            f"<img src='{video_url}.jpg' alt='Watch video' "
            f"style='max-width:480px;border-radius:8px;'/></a></p>"
            f"<p><a href='{video_url}'>Open the video</a></p>"
            f"<p>â€” Chuck</p>"
            f"</body></html>"
        )
        email_ok = ghl_send_email(contact_id, subject, html)

    # --- 7. GHL side-effects + RDS -----------------------------------------
    now_iso = datetime.now(timezone.utc).isoformat()
    ghl_update_custom_fields(
        contact_id,
        {
            "VIDEO_LAST_SENT": now_iso,
            "VIDEO_LAST_TYPE": video_type,
            "VIDEO_LAST_URL": video_url,
        },
    )
    ghl_add_tag(contact_id, f"VIDEO:{video_type.upper()}_SENT")

    rds_write_delivery(
        contact_id,
        video_type,
        heygen_video_id,
        video_url,
        sms_ok,
        email_ok,
        generation_ms,
    )

    log.info(
        "video delivered hash=%s type=%s gen_ms=%d sms=%s email=%s",
        _hash_contact(contact_id),
        video_type,
        generation_ms,
        sms_ok,
        email_ok,
    )

    return {
        "ok": True,
        "status": "delivered",
        "video_type": video_type,
        "heygen_video_id": heygen_video_id,
        "video_url": video_url,
        "sms_delivered": sms_ok,
        "email_delivered": email_ok,
        "generation_ms": generation_ms,
        "elapsed_ms": int((time.time() - t0) * 1000),
    }


# ---------------------------------------------------------------------------
# AgentCore entrypoint
# ---------------------------------------------------------------------------
@app.entrypoint
def invoke(payload: dict) -> dict:
    """
    Payload shape:
      {
        "path": "/video/generate",
        "video_type": "hot_lead",
        "contact_id": "ghl_abc123",
        "first_name": "Mike",
        "city": "Phoenix",
        "business_name": "Phoenix HVAC Pro",
        "primary_gap": "map_pack",
        "ranking_keywords": ["hvac phoenix"],
        "delivery_channels": ["sms", "email"]
      }
    """
    try:
        path = (payload.get("path") or "/video/generate").rstrip("/")
        if path == "/video/generate":
            return handle_generate(payload)
        if path in ("/ping", "/health"):
            return {
                "ok": True,
                "agent": "videoEngine",
                "status": "healthy",
                "video_types": list(VIDEO_TYPES.keys()),
            }
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
