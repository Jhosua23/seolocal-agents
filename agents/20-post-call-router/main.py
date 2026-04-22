"""
================================================================
Agent 20 — Post-Call Router
================================================================
AgentCore name  : postCallRouter
Phase           : 2 — Conversion & Retention Engine
Module          : 4 — Demo Conversion
Wave            : 1
Priority        : P0
Runtime         : Python 3.12 / ARM64
Version         : 1.0.0

NO RDS — outcome logged as GHL custom fields + tags.
Routes every demo outcome to correct next action.

3 PATHS:
  CLOSED_WON  → Recurly checkout URL → SMS + email
                → Slack win alert → GHL tags
  CLOSED_LOST → log loss reason → cold-nurture tag
                → move to Resurrection pipeline
                → Slack alert
  NO_SHOW     → rebook SMS → increment no-show count
                → GHL tag → Slack escalation at >=2

================================================================
SSM Parameters required:
  Already in your SSM:
    GHL_API_KEY              - SecureString
    GHL_STAGE_ACTIVE_CLIENT  - String
    GHL_DEFAULT_ASSIGNEE_ID  - String

  Ask Chuck to add (Phase 2 new):
    GHL_CALENDAR_BOOKING_URL           - String
    GHL_PIPELINE_RESURRECTION_ID       - String
    GHL_STAGE_RESURRECTION_COLD_NURTURE - String
    RECURLY_API_KEY                    - SecureString
    SLACK_BOT_TOKEN                    - SecureString
    SLACK_CHANNEL_COMMAND_CENTER       - String
    HEYGEN_CLOSED_WON_ACTIVE           - String ("false")
    LOCATION_ID                        - String
================================================================
"""

import hashlib
import json
import logging
import time
from datetime import datetime, timedelta, timezone

import boto3
import requests

# ----------------------------------------------------------------
# Logging — structured JSON for CloudWatch
# ----------------------------------------------------------------
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def log(level: str, event: str, **kwargs) -> None:
    record = {
        "level":   level,
        "event":   event,
        "ts":      datetime.now(timezone.utc).isoformat(),
        "agent":   "postCallRouter",
        "version": "1.0.0",
    }
    record.update(kwargs)
    getattr(logger, level.lower())(json.dumps(record))


# ----------------------------------------------------------------
# AgentCore SDK
# ----------------------------------------------------------------
try:
    from bedrock_agentcore.runtime import BedrockAgentCoreApp
    _SDK = True
except ImportError:
    _SDK = False


# ----------------------------------------------------------------
# Constants
# ----------------------------------------------------------------
GHL_BASE     = "https://services.leadconnectorhq.com"
REGION       = "us-east-1"
CODE_VERSION = "1.0.0"

# Valid outcomes
VALID_OUTCOMES = {"CLOSED_WON", "CLOSED_LOST", "NO_SHOW"}

# Tags
TAG_CLOSED_WON      = "MOD:CLOSED_WON"
TAG_CLOSED_LOST     = "MOD:CLOSED_LOST"
TAG_CHECKOUT_SENT   = "checkout-link-sent"
TAG_COLD_NURTURE    = "cold-nurture"
TAG_NO_SHOW         = "no-show"

# MRR map — plan code to monthly value
PLAN_MRR = {
    "seolocal-pro":       797,
    "seolocal-starter":   497,
    "seolocal-elite":    1497,
    "seolocal-agency":   2497,
}


# ----------------------------------------------------------------
# SSM — cached, NextToken pagination mandatory per dev guide
# ----------------------------------------------------------------
_ssm_cache: dict = {}


def _ssm_client():
    return boto3.client("ssm", region_name=REGION)


def get_ssm(name: str, default: str = "") -> str:
    if name in _ssm_cache:
        return _ssm_cache[name]
    try:
        val = _ssm_client().get_parameter(
            Name=name, WithDecryption=True
        )["Parameter"]["Value"]
        _ssm_cache[name] = val
        return val
    except Exception:
        return default


# ----------------------------------------------------------------
# GHL HTTP helper — rate-limit aware with retry
# ----------------------------------------------------------------
def _ghl(method: str, url: str, retries: int = 2, **kwargs):
    headers = {
        "Authorization": f"Bearer {get_ssm('GHL_API_KEY')}",
        "Version":       "2021-07-28",
        "Content-Type":  "application/json",
    }
    for attempt in range(retries + 1):
        try:
            resp = requests.request(
                method, url, headers=headers, timeout=10, **kwargs
            )
        except requests.RequestException as exc:
            log("error", "ghl_request_exception",
                url=url, error=str(exc), attempt=attempt)
            if attempt == retries:
                raise
            time.sleep(2 ** attempt)
            continue

        if resp.status_code == 401:
            raise RuntimeError("GHL auth failed — check GHL_API_KEY in SSM")
        if resp.status_code == 429:
            log("warning", "ghl_rate_limited", url=url)
            time.sleep(10)
            continue
        if resp.status_code >= 500:
            if attempt == retries:
                raise RuntimeError(f"GHL 5xx: {resp.status_code}")
            time.sleep(2 ** attempt)
            continue

        return resp

    raise RuntimeError(f"GHL request failed: {url}")


# ----------------------------------------------------------------
# GHL — Get contact
# ----------------------------------------------------------------
def ghl_get_contact(contact_id: str) -> dict:
    resp = _ghl("GET", f"{GHL_BASE}/contacts/{contact_id}")
    if resp.status_code == 200:
        return resp.json().get("contact", {})
    log("warning", "ghl_get_contact_failed",
        contact_id_hash=_h(contact_id), status=resp.status_code)
    return {}


# ----------------------------------------------------------------
# GHL — Add tags
# ----------------------------------------------------------------
def ghl_add_tags(contact_id: str, tags: list) -> bool:
    resp = _ghl(
        "POST",
        f"{GHL_BASE}/contacts/{contact_id}/tags",
        json={"tags": tags},
    )
    ok = resp.status_code in (200, 201)
    if not ok:
        log("warning", "ghl_add_tags_failed",
            contact_id_hash=_h(contact_id),
            tags=tags, status=resp.status_code)
    return ok


# ----------------------------------------------------------------
# GHL — Update custom fields
# ----------------------------------------------------------------
def ghl_update_fields(contact_id: str, fields: list) -> bool:
    resp = _ghl(
        "PUT",
        f"{GHL_BASE}/contacts/{contact_id}",
        json={"customFields": fields},
    )
    ok = resp.status_code in (200, 201)
    if not ok:
        log("warning", "ghl_update_fields_failed",
            contact_id_hash=_h(contact_id), status=resp.status_code)
    return ok


# ----------------------------------------------------------------
# GHL — Send SMS
# ----------------------------------------------------------------
def ghl_send_sms(contact_id: str, message: str) -> bool:
    resp = _ghl(
        "POST",
        f"{GHL_BASE}/conversations/messages",
        json={
            "type":      "SMS",
            "message":   message,
            "contactId": contact_id,
        },
    )
    ok = resp.status_code in (200, 201)
    if not ok:
        log("warning", "ghl_send_sms_failed",
            contact_id_hash=_h(contact_id), status=resp.status_code)
    return ok


# ----------------------------------------------------------------
# GHL — Send Email
# ----------------------------------------------------------------
def ghl_send_email(contact_id: str, subject: str, html_body: str) -> bool:
    resp = _ghl(
        "POST",
        f"{GHL_BASE}/conversations/messages",
        json={
            "type":    "Email",
            "subject": subject,
            "html":    html_body,
            "to":      [{"type": "contact", "contactId": contact_id}],
        },
    )
    ok = resp.status_code in (200, 201)
    if not ok:
        log("warning", "ghl_send_email_failed",
            contact_id_hash=_h(contact_id), status=resp.status_code)
    return ok


# ----------------------------------------------------------------
# GHL — Create task (for Chuck on 2+ no-shows)
# ----------------------------------------------------------------
def ghl_create_task(contact_id: str, title: str, body: str) -> bool:
    resp = _ghl(
        "POST",
        f"{GHL_BASE}/contacts/{contact_id}/tasks",
        json={
            "title":      title,
            "body":       body,
            "assignedTo": get_ssm("GHL_DEFAULT_ASSIGNEE_ID"),
            "contactId":  contact_id,
            "dueDate":    (datetime.now(timezone.utc) + timedelta(hours=2)).isoformat() + "Z",
            "completed":  False,
        },
    )
    ok = resp.status_code in (200, 201)
    if not ok:
        log("warning", "ghl_create_task_failed",
            contact_id_hash=_h(contact_id), status=resp.status_code)
    return ok


# ----------------------------------------------------------------
# GHL — Move to Resurrection pipeline (CLOSED_LOST)
# ----------------------------------------------------------------
def ghl_move_to_resurrection(contact_id: str, business_name: str) -> bool:
    pipeline_id = get_ssm("GHL_PIPELINE_RESURRECTION_ID", "")
    stage_id    = get_ssm("GHL_STAGE_RESURRECTION_COLD_NURTURE", "")

    if not pipeline_id or not stage_id:
        log("warning", "resurrection_pipeline_not_configured",
            contact_id_hash=_h(contact_id),
            note="GHL_PIPELINE_RESURRECTION_ID or GHL_STAGE_RESURRECTION_COLD_NURTURE not in SSM")
        return False

    # Search for existing opportunity
    search = _ghl(
        "GET",
        f"{GHL_BASE}/opportunities/search",
        params={"contact_id": contact_id, "pipeline_id": pipeline_id},
    )
    opps = search.json().get("opportunities", []) if search.status_code == 200 else []

    if opps:
        opp_id = opps[0]["id"]
    else:
        # Create new opportunity in resurrection pipeline
        resp = _ghl(
            "POST",
            f"{GHL_BASE}/opportunities/",
            json={
                "pipelineId":      pipeline_id,
                "pipelineStageId": stage_id,
                "contactId":       contact_id,
                "name":            f"{business_name} — Cold Nurture",
                "status":          "open",
            },
        )
        if resp.status_code not in (200, 201):
            log("warning", "ghl_create_resurrection_opp_failed",
                contact_id_hash=_h(contact_id), status=resp.status_code)
            return False
        return True

    # Move existing opportunity to cold nurture stage
    resp = _ghl(
        "PUT",
        f"{GHL_BASE}/opportunities/{opp_id}",
        json={
            "pipelineId":      pipeline_id,
            "pipelineStageId": stage_id,
            "status":          "open",
        },
    )
    ok = resp.status_code in (200, 201)
    if not ok:
        log("warning", "ghl_move_resurrection_failed",
            opp_id=opp_id, status=resp.status_code)
    return ok


# ----------------------------------------------------------------
# Recurly — Generate hosted checkout URL
# ----------------------------------------------------------------
def recurly_generate_checkout_url(
    contact_id: str,
    email: str,
    plan_code: str,
    first_name: str,
) -> str:
    """
    Generates a Recurly hosted checkout URL for the given plan.
    Uses Recurly REST API v3 — REST via requests (not gRPC).
    Falls back to a base checkout URL if API call fails.
    """
    api_key = get_ssm("RECURLY_API_KEY", "")

    if not api_key:
        log("warning", "recurly_api_key_missing",
            note="RECURLY_API_KEY not in SSM — using fallback URL")
        return f"https://app.recurly.com/subscribe/{plan_code}"

    try:
        resp = requests.post(
            "https://v3.recurly.com/checkout-sessions",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Accept":        "application/vnd.recurly.v2021-02-25",
                "Content-Type":  "application/json",
            },
            json={
                "cancel_url":  "https://seolocal.us/checkout-cancelled",
                "success_url": "https://seolocal.us/welcome",
                "currency":    "USD",
                "line_items": [
                    {
                        "plan_code": plan_code,
                        "quantity":  1,
                    }
                ],
                "customer_notes": f"Contact ID: {contact_id}",
                "account": {
                    "email":      email,
                    "first_name": first_name,
                    "code":       contact_id,
                },
            },
            timeout=10,
        )

        if resp.status_code in (200, 201):
            url = resp.json().get("url", "")
            if url:
                log("info", "recurly_checkout_url_generated",
                    contact_id_hash=_h(contact_id), plan_code=plan_code)
                return url

        log("warning", "recurly_checkout_failed",
            status=resp.status_code, body=resp.text[:200])

    except Exception as exc:
        log("error", "recurly_exception", error=str(exc))

    # Fallback — basic plan URL
    return f"https://app.recurly.com/subscribe/{plan_code}"


# ----------------------------------------------------------------
# Slack — Post alert message
# ----------------------------------------------------------------
def slack_post(message: str) -> bool:
    """
    Posts a message to the command center Slack channel.
    Uses Slack Web API — REST via requests (not gRPC).
    """
    token      = get_ssm("SLACK_BOT_TOKEN", "")
    channel_id = get_ssm("SLACK_CHANNEL_COMMAND_CENTER", "")

    if not token or not channel_id:
        log("warning", "slack_not_configured",
            note="SLACK_BOT_TOKEN or SLACK_CHANNEL_COMMAND_CENTER not in SSM")
        return False

    try:
        resp = requests.post(
            "https://slack.com/api/chat.postMessage",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type":  "application/json",
            },
            json={
                "channel": channel_id,
                "text":    message,
            },
            timeout=10,
        )
        data = resp.json()
        ok   = data.get("ok", False)
        if not ok:
            log("warning", "slack_post_failed", error=data.get("error", "unknown"))
        return ok
    except Exception as exc:
        log("error", "slack_exception", error=str(exc))
        return False


# ----------------------------------------------------------------
# HTML email builder
# ----------------------------------------------------------------
def _html(plain: str) -> str:
    lines = plain.strip().split("\n")
    body  = "".join(
        f"<p style='margin:0 0 12px 0'>{l if l.strip() else '&nbsp;'}</p>"
        for l in lines
    )
    return (
        "<div style='font-family:Arial,Helvetica,sans-serif;"
        "font-size:15px;line-height:1.6;color:#1a1a1a;"
        "max-width:600px;margin:0 auto'>"
        f"{body}</div>"
    )


# ----------------------------------------------------------------
# Helper
# ----------------------------------------------------------------
def _h(contact_id: str) -> str:
    return hashlib.sha256(str(contact_id).encode()).hexdigest()[:16]


def _get_field(contact: dict, key: str) -> str:
    for f in contact.get("customFields", []):
        if f.get("key") == key:
            return str(f.get("value") or "")
    return ""


def _get_mrr(plan_code: str) -> int:
    return PLAN_MRR.get(plan_code, 797)


# ----------------------------------------------------------------
# PATH 1 — CLOSED WON
# ----------------------------------------------------------------
def handle_closed_won(
    contact_id: str,
    first_name: str,
    email: str,
    phone: str,
    plan_code: str,
    contact: dict,
) -> dict:
    log("info", "path_closed_won",
        contact_id_hash=_h(contact_id), plan_code=plan_code)

    business_name = contact.get("companyName") or contact.get("firstName", "your business")
    city          = _get_field(contact, "city") or "your area"
    mrr           = _get_mrr(plan_code)

    # Step 1: Generate Recurly checkout URL
    checkout_url = recurly_generate_checkout_url(
        contact_id, email, plan_code, first_name
    )

    # Step 2: Send payment link via SMS
    sms_text = (
        f"Congrats {first_name}! 🎉 Here's your secure checkout link to get started "
        f"with SEO Local: {checkout_url} — takes 2 minutes. "
        f"Questions? Reply to this message."
    )
    sms_ok = ghl_send_sms(contact_id, sms_text)

    # Step 3: Send payment link via email
    email_subject = f"You're in, {first_name} — here's your checkout link"
    email_body = (
        f"Hey {first_name},\n\n"
        f"Great news — you're all set to get started with SEO Local!\n\n"
        f"Click below to complete your setup (takes about 2 minutes):\n"
        f"{checkout_url}\n\n"
        f"What happens next:\n"
        f"  → Payment confirmed → onboarding call scheduled within 24 hours\n"
        f"  → Your dedicated SEO team starts work on Day 3\n"
        f"  → First ranking report in 30 days\n\n"
        f"Any questions? Just reply to this email.\n\n"
        f"— The SEO Local Team"
    )
    email_ok = ghl_send_email(contact_id, email_subject, _html(email_body))

    # Step 4: Slack win alert
    slack_msg = (
        f"🏆 CLOSED WON\n"
        f"Name     : {first_name} | {business_name}\n"
        f"City     : {city}\n"
        f"Plan     : {plan_code}\n"
        f"MRR      : ${mrr}/mo\n"
        f"Checkout : {checkout_url}"
    )
    slack_post(slack_msg)

    # Step 5: Phase 2 video — only if feature flag is true
    video_triggered = False
    if get_ssm("HEYGEN_CLOSED_WON_ACTIVE", "false").lower() == "true":
        video_endpoint = get_ssm("AGENT_22_ENDPOINT", "")
        if video_endpoint:
            try:
                requests.post(
                    video_endpoint,
                    json={
                        "contact_id":  contact_id,
                        "video_type":  "closed_won",
                        "first_name":  first_name,
                        "business_name": business_name,
                    },
                    timeout=15,
                )
                video_triggered = True
                log("info", "video_engine_triggered",
                    contact_id_hash=_h(contact_id))
            except Exception as exc:
                log("warning", "video_engine_failed", error=str(exc))

    # Step 6: Apply GHL tags + update fields
    ghl_add_tags(contact_id, [TAG_CLOSED_WON, TAG_CHECKOUT_SENT])
    ghl_update_fields(contact_id, [
        {"key": "checkout_url",          "field_value": checkout_url},
        {"key": "closed_won_at",         "field_value": datetime.now(timezone.utc).isoformat()},
        {"key": "plan_code",             "field_value": plan_code},
        {"key": "payment_link_sent",     "field_value": "true"},
        {"key": "demo_outcome",          "field_value": "CLOSED_WON"},
    ])

    log("info", "closed_won_complete",
        contact_id_hash=_h(contact_id),
        plan_code=plan_code,
        mrr=mrr,
        sms_ok=sms_ok,
        email_ok=email_ok,
        video_triggered=video_triggered,
        checkout_url_generated=bool(checkout_url))

    return {
        "status":          "routed",
        "outcome":         "CLOSED_WON",
        "plan_code":       plan_code,
        "mrr":             mrr,
        "checkout_url":    checkout_url,
        "sms_sent":        sms_ok,
        "email_sent":      email_ok,
        "video_triggered": video_triggered,
    }


# ----------------------------------------------------------------
# PATH 2 — CLOSED LOST
# ----------------------------------------------------------------
def handle_closed_lost(
    contact_id: str,
    first_name: str,
    loss_reason: str,
    contact: dict,
) -> dict:
    log("info", "path_closed_lost",
        contact_id_hash=_h(contact_id), loss_reason=loss_reason)

    business_name = contact.get("companyName") or contact.get("firstName", "Unknown")
    city          = _get_field(contact, "city") or "unknown city"
    reason_clean  = loss_reason or "No reason provided"

    # Step 1: Log loss reason to GHL custom field
    ghl_update_fields(contact_id, [
        {"key": "loss_reason",    "field_value": reason_clean},
        {"key": "demo_outcome",   "field_value": "CLOSED_LOST"},
        {"key": "closed_lost_at", "field_value": datetime.now(timezone.utc).isoformat()},
    ])

    # Step 2: Apply tags — cold-nurture tag fires Agent 24 via GHL webhook
    ghl_add_tags(contact_id, [TAG_CLOSED_LOST, TAG_COLD_NURTURE])

    # Step 3: Move to Resurrection pipeline Cold Nurture stage
    moved = ghl_move_to_resurrection(contact_id, business_name)

    # Step 4: Slack alert
    slack_msg = (
        f"❌ CLOSED LOST\n"
        f"Name   : {first_name} | {business_name}\n"
        f"City   : {city}\n"
        f"Reason : {reason_clean}\n"
        f"Action : Entering cold nurture sequence (Agent 24)"
    )
    slack_post(slack_msg)

    log("info", "closed_lost_complete",
        contact_id_hash=_h(contact_id),
        loss_reason=reason_clean,
        moved_to_resurrection=moved)

    return {
        "status":               "routed",
        "outcome":              "CLOSED_LOST",
        "loss_reason":          reason_clean,
        "moved_to_resurrection": moved,
        "cold_nurture_tagged":  True,
    }


# ----------------------------------------------------------------
# PATH 3 — NO SHOW
# ----------------------------------------------------------------
def handle_no_show(
    contact_id: str,
    first_name: str,
    demo_no_show_count: int,
    contact: dict,
) -> dict:
    log("info", "path_no_show",
        contact_id_hash=_h(contact_id),
        no_show_count=demo_no_show_count)

    business_name  = contact.get("companyName") or contact.get("firstName", "Unknown")
    calendar_url   = get_ssm("GHL_CALENDAR_BOOKING_URL", "https://seolocal.us/book")
    new_count      = demo_no_show_count + 1

    # Step 1: Send re-book SMS
    sms_text = (
        f"Hey {first_name}, we missed you on today's call! No worries — "
        f"grab a new time here: {calendar_url} "
        f"We look forward to connecting soon."
    )
    sms_ok = ghl_send_sms(contact_id, sms_text)

    # Step 2: Increment no-show count in GHL
    ghl_update_fields(contact_id, [
        {"key": "demo_no_show_count", "field_value": str(new_count)},
        {"key": "last_no_show_at",    "field_value": datetime.now(timezone.utc).isoformat()},
        {"key": "demo_outcome",       "field_value": "NO_SHOW"},
    ])

    # Step 3: Apply no-show tag
    ghl_add_tags(contact_id, [TAG_NO_SHOW])

    # Step 4: Escalate if >= 2 no-shows
    escalated = False
    if new_count >= 2:
        slack_msg = (
            f"⚠️ NO-SHOW x{new_count}\n"
            f"Name     : {first_name} | {business_name}\n"
            f"Missed   : {new_count} demos — escalating to Chuck\n"
            f"Action   : Manual follow-up required"
        )
        slack_post(slack_msg)

        ghl_create_task(
            contact_id,
            f"ESCALATION — {new_count}x No-Show: {business_name}",
            f"{first_name} from {business_name} has missed {new_count} demo calls. "
            f"Manual personal outreach required. "
            f"Last no-show: {datetime.now(timezone.utc).strftime('%B %d, %Y')}",
        )
        escalated = True
        log("warning", "no_show_escalated",
            contact_id_hash=_h(contact_id), count=new_count)

    log("info", "no_show_complete",
        contact_id_hash=_h(contact_id),
        no_show_count=new_count,
        sms_ok=sms_ok,
        escalated=escalated)

    return {
        "status":      "routed",
        "outcome":     "NO_SHOW",
        "no_show_count": new_count,
        "sms_sent":    sms_ok,
        "escalated":   escalated,
    }


# ----------------------------------------------------------------
# CORE AGENT LOGIC
# ----------------------------------------------------------------
def run_post_call_router(payload: dict) -> dict:

    # --- Validate required fields ---
    contact_id = (payload.get("contact_id") or "").strip()
    outcome    = (payload.get("outcome") or "").strip().upper()
    email      = (payload.get("email") or "").strip()
    first_name = (payload.get("first_name") or "there").strip()
    phone      = (payload.get("phone") or "").strip()

    if not contact_id:
        return {"status": "error", "code": "MISSING_CONTACT_ID"}
    if outcome not in VALID_OUTCOMES:
        return {
            "status": "error",
            "code":   "INVALID_OUTCOME",
            "valid":  list(VALID_OUTCOMES),
            "received": outcome,
        }

    log("info", "router_start",
        contact_id_hash=_h(contact_id),
        outcome=outcome)

    # --- Fetch full GHL contact for enrichment ---
    contact = ghl_get_contact(contact_id)
    if not contact:
        log("error", "contact_not_found", contact_id_hash=_h(contact_id))
        return {"status": "error", "code": "CONTACT_NOT_FOUND"}

    # --- Route by outcome ---
    if outcome == "CLOSED_WON":
        plan_code = (payload.get("plan_code") or "seolocal-pro").strip()
        result = handle_closed_won(
            contact_id, first_name, email, phone, plan_code, contact
        )

    elif outcome == "CLOSED_LOST":
        loss_reason = (payload.get("loss_reason") or "").strip()
        result = handle_closed_lost(
            contact_id, first_name, loss_reason, contact
        )

    else:  # NO_SHOW
        no_show_count = int(payload.get("demo_no_show_count") or 0)
        result = handle_no_show(
            contact_id, first_name, no_show_count, contact
        )

    # --- Final CloudWatch log ---
    log("info", "router_complete",
        contact_id_hash=_h(contact_id),
        outcome=outcome,
        version=CODE_VERSION)

    result["contact_id"] = contact_id
    result["agent"]      = "postCallRouter"
    result["version"]    = CODE_VERSION
    return result


# ----------------------------------------------------------------
# AgentCore entrypoint
# Rules:
#   1. payload: dict only — no context param (causes HTTP 400)
#   2. app.run() is mandatory — agent times out without it
#   3. SystemExit and BaseException caught separately
# ----------------------------------------------------------------
if _SDK:
    app = BedrockAgentCoreApp()

    @app.entrypoint
    def post_call_router(payload: dict) -> dict:
        try:
            return run_post_call_router(payload)
        except SystemExit:
            raise
        except BaseException as exc:
            log("error", "unhandled_exception",
                error=str(exc), exc_type=type(exc).__name__)
            return {
                "status":  "error",
                "code":    "INTERNAL_ERROR",
                "message": str(exc),
            }

    app.run()
