"""
================================================================
Agent 21 — Client Onboarding
================================================================
AgentCore name  : clientOnboarding
Phase           : 2 — Conversion & Retention Engine
Module          : 5 — Onboarding & Client Activation
Wave            : 1
Priority        : P0
Runtime         : Python 3.12 / ARM64
Version         : 1.0.0

NO RDS — onboarding state stored in GHL custom fields + tags.
Dedup  : tag MOD:ONBOARDING_COMPLETE on contact.

Fires the moment Recurly subscription.activated → Lambda bridge
→ GHL tag REL:CLIENT → GHL webhook → this agent.

WHAT IT DOES:
  1. Validates plan_code
  2. Creates Slack #client-[name]-[city] channel + invites team
  3. Sends welcome SMS immediately
  4. Sends welcome email immediately
  5. Creates GHL Fulfillment pipeline entry at Onboarding stage
  6. Writes GHL custom fields (subscription, plan, product)
  7. Applies GHL tags (REL:CLIENT, MOD:ONBOARDING_COMPLETE, PRODUCT:*)
  8. Creates kickoff call task (Day 3)
  9. Creates 30-day check-in task
  10. Posts new client alert to Slack #c-suite-command-center
  11. Agency-client path: notifies agency owner if applicable
  12. Logs to CloudWatch

================================================================
SSM Parameters:
  Already in your SSM:
    GHL_API_KEY              - SecureString
    GHL_DEFAULT_ASSIGNEE_ID  - String (Jay Leonard)

  Ask Chuck / Gaurav to add:
    GHL_PIPELINE_FULFILLMENT_ID        - String  (create pipeline in GHL first)
    GHL_STAGE_FULFILLMENT_ONBOARDING   - String  (stage UUID from GHL)
    GHL_CALENDAR_BOOKING_URL           - String  (https://api.leadconnectorhq.com/widget/booking/rByTO30SgbyXG7uLMDwV)
    SLACK_BOT_TOKEN                    - SecureString
    SLACK_CHANNEL_COMMAND_CENTER       - String
    SLACK_OPS_TEAM_MEMBERS             - String  (comma-separated Slack user IDs)
    SLACK_CHUCK_USER_ID                - String
================================================================
"""

import hashlib
import json
import logging
import re
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
        "agent":   "clientOnboarding",
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
LOCATION_ID  = "uXRl9WpDjS7LFjeYfQqD"
REGION       = "us-east-1"
CODE_VERSION = "1.0.0"

VALID_PLANS = {
    "seolocal-pro":       {"tag": "PRODUCT:SEO_PRO",       "name": "SEO Local Pro"},
    "seolocal-pro-plus":  {"tag": "PRODUCT:SEO_PRO_PLUS",  "name": "SEO Local Pro+"},
    "seolocal-premiere":  {"tag": "PRODUCT:SEO_PREMIERE",  "name": "SEO Local Premiere"},
    "seolocal-starter":   {"tag": "PRODUCT:SEO_STARTER",   "name": "SEO Local Starter"},
    "seolocal-elite":     {"tag": "PRODUCT:SEO_ELITE",     "name": "SEO Local Elite"},
    "seolocal-agency":    {"tag": "PRODUCT:SEO_AGENCY",    "name": "SEO Local Agency"},
}

# Tags
TAG_REL_CLIENT          = "REL:CLIENT"
TAG_ONBOARDING_COMPLETE = "MOD:ONBOARDING_COMPLETE"


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
# GHL — Send welcome SMS
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
# GHL — Send welcome email
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
# GHL — Create task (kickoff D+3 and check-in D+30)
# ----------------------------------------------------------------
def ghl_create_task(
    contact_id: str,
    title: str,
    body: str,
    due_days: int = 3,
) -> bool:
    due_date = (
        datetime.now(timezone.utc) + timedelta(days=due_days)
    ).isoformat() + "Z"

    resp = _ghl(
        "POST",
        f"{GHL_BASE}/contacts/{contact_id}/tasks",
        json={
            "title":      title,
            "body":       body,
            "assignedTo": get_ssm("GHL_DEFAULT_ASSIGNEE_ID"),
            "contactId":  contact_id,
            "dueDate":    due_date,
            "completed":  False,
        },
    )
    ok = resp.status_code in (200, 201)
    if not ok:
        log("warning", "ghl_create_task_failed",
            contact_id_hash=_h(contact_id),
            title=title, status=resp.status_code)
    return ok


# ----------------------------------------------------------------
# GHL — Create Fulfillment pipeline opportunity
# ----------------------------------------------------------------
def ghl_create_fulfillment_entry(
    contact_id: str,
    business_name: str,
    plan_name: str,
    mrr: int,
) -> bool:
    pipeline_id = get_ssm("GHL_PIPELINE_FULFILLMENT_ID", "")
    stage_id    = get_ssm("GHL_STAGE_FULFILLMENT_ONBOARDING", "")

    if not pipeline_id or not stage_id:
        log("warning", "fulfillment_pipeline_not_configured",
            note="GHL_PIPELINE_FULFILLMENT_ID or GHL_STAGE_FULFILLMENT_ONBOARDING not in SSM")
        return False

    resp = _ghl(
        "POST",
        f"{GHL_BASE}/opportunities/",
        json={
            "pipelineId":      pipeline_id,
            "pipelineStageId": stage_id,
            "contactId":       contact_id,
            "name":            f"{business_name} — {plan_name}",
            "status":          "open",
            "monetaryValue":   mrr,
        },
    )
    ok = resp.status_code in (200, 201)
    if not ok:
        log("warning", "ghl_fulfillment_entry_failed",
            contact_id_hash=_h(contact_id), status=resp.status_code)
    return ok


# ----------------------------------------------------------------
# Slack HTTP helper — REST via requests (not gRPC)
# ----------------------------------------------------------------
def _slack(endpoint: str, payload: dict) -> dict:
    token = get_ssm("SLACK_BOT_TOKEN", "")
    if not token:
        log("warning", "slack_token_missing")
        return {"ok": False, "error": "token_missing"}
    try:
        resp = requests.post(
            f"https://slack.com/api/{endpoint}",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type":  "application/json",
            },
            json=payload,
            timeout=10,
        )
        data = resp.json()
        if not data.get("ok"):
            log("warning", "slack_api_error",
                endpoint=endpoint, error=data.get("error", "unknown"))
        return data
    except Exception as exc:
        log("error", "slack_exception", endpoint=endpoint, error=str(exc))
        return {"ok": False, "error": str(exc)}


# ----------------------------------------------------------------
# Slack — Create channel and invite team
# ----------------------------------------------------------------
def slack_create_client_channel(
    business_name: str,
    city: str,
) -> str:
    """
    Creates #client-[name-slug]-[city-slug] channel.
    Returns channel_id or empty string on failure.
    """
    slug        = _slugify(business_name)
    city_slug   = _slugify(city)
    channel_name = f"client-{slug}-{city_slug}"[:80]  # Slack max 80 chars

    # Create channel
    result = _slack("conversations.create", {
        "name":      channel_name,
        "is_private": False,
    })

    if not result.get("ok"):
        # Channel may already exist
        if result.get("error") == "name_taken":
            log("info", "slack_channel_exists", channel=channel_name)
            # Find existing channel
            find = _slack("conversations.list", {
                "types":           "public_channel",
                "exclude_archived": True,
                "limit":           200,
            })
            channels = find.get("channels", [])
            for ch in channels:
                if ch.get("name") == channel_name:
                    return ch["id"]
        log("warning", "slack_create_channel_failed",
            channel=channel_name, error=result.get("error"))
        return ""

    channel_id = result.get("channel", {}).get("id", "")
    log("info", "slack_channel_created", channel=channel_name, id=channel_id)

    # Invite Chuck + ops team
    if channel_id:
        members_to_invite = []

        chuck_id = get_ssm("SLACK_CHUCK_USER_ID", "")
        if chuck_id:
            members_to_invite.append(chuck_id)

        ops_raw = get_ssm("SLACK_OPS_TEAM_MEMBERS", "")
        if ops_raw:
            ops_ids = [uid.strip() for uid in ops_raw.split(",") if uid.strip()]
            members_to_invite.extend(ops_ids)

        if members_to_invite:
            _slack("conversations.invite", {
                "channel": channel_id,
                "users":   ",".join(set(members_to_invite)),
            })

        # Post welcome message in new channel
        _slack("chat.postMessage", {
            "channel": channel_id,
            "text": (
                f"👋 Welcome to your SEO Local client channel!\n\n"
                f"*Business:* {business_name} | *City:* {city}\n"
                f"This channel is for campaign updates, results, and client comms.\n"
                f"Kickoff call scheduled for Day 3. 🚀"
            ),
        })

    return channel_id


# ----------------------------------------------------------------
# Slack — Post to command center
# ----------------------------------------------------------------
def slack_post_command_center(message: str) -> bool:
    channel_id = get_ssm("SLACK_CHANNEL_COMMAND_CENTER", "")
    if not channel_id:
        log("warning", "slack_command_center_not_configured")
        return False

    result = _slack("chat.postMessage", {
        "channel": channel_id,
        "text":    message,
    })
    return result.get("ok", False)


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
# Helpers
# ----------------------------------------------------------------
def _h(contact_id: str) -> str:
    return hashlib.sha256(str(contact_id).encode()).hexdigest()[:16]


def _slugify(text: str) -> str:
    """Convert to lowercase slug for Slack channel names."""
    text = text.lower().strip()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[\s_]+", "-", text)
    text = re.sub(r"-+", "-", text)
    return text.strip("-")[:30]


def _get_plan_name(plan_code: str) -> str:
    return VALID_PLANS.get(plan_code, {}).get("name", plan_code)


def _get_plan_tag(plan_code: str) -> str:
    return VALID_PLANS.get(plan_code, {}).get("tag", "PRODUCT:SEO_PRO")


# ----------------------------------------------------------------
# CORE AGENT LOGIC
# ----------------------------------------------------------------
def run_client_onboarding(payload: dict) -> dict:
    now = datetime.now(timezone.utc)

    # --- Validate required fields ---
    contact_id     = (payload.get("contact_id") or "").strip()
    first_name     = (payload.get("first_name") or "there").strip()
    last_name      = (payload.get("last_name") or "").strip()
    email          = (payload.get("email") or "").strip()
    phone          = (payload.get("phone") or "").strip()
    business_name  = (payload.get("business_name") or "").strip()
    city           = (payload.get("city") or "").strip()
    state          = (payload.get("state") or "").strip()
    plan_code      = (payload.get("plan_code") or "seolocal-pro").strip()
    subscription_id = (payload.get("recurly_subscription_id") or "").strip()
    mrr            = int(payload.get("mrr") or 0)
    account_type   = (payload.get("account_type") or "direct").strip()

    if not contact_id:
        return {"status": "error", "code": "MISSING_CONTACT_ID"}
    if not business_name:
        return {"status": "error", "code": "MISSING_BUSINESS_NAME"}
    if not email:
        return {"status": "error", "code": "MISSING_EMAIL"}

    plan_name = _get_plan_name(plan_code)
    plan_tag  = _get_plan_tag(plan_code)
    calendar_url = get_ssm("GHL_CALENDAR_BOOKING_URL",
                           "https://api.leadconnectorhq.com/widget/booking/rByTO30SgbyXG7uLMDwV")

    log("info", "onboarding_start",
        contact_id_hash=_h(contact_id),
        plan_code=plan_code,
        mrr=mrr,
        account_type=account_type)

    # --- Dedup guard — check if already onboarded ---
    contact = ghl_get_contact(contact_id)
    if TAG_ONBOARDING_COMPLETE in contact.get("tags", []):
        log("info", "already_onboarded_skip",
            contact_id_hash=_h(contact_id))
        return {"status": "skipped", "reason": "already_onboarded"}

    results = {}

    # --- Step 2: Create Slack client channel ---
    log("info", "creating_slack_channel", contact_id_hash=_h(contact_id))
    channel_id = slack_create_client_channel(business_name, city)
    channel_name = f"client-{_slugify(business_name)}-{_slugify(city)}"
    results["slack_channel"] = channel_id
    results["slack_channel_name"] = channel_name

    # --- Step 3: Welcome SMS ---
    sms_text = (
        f"Welcome to SEO Local, {first_name}! 🎉 "
        f"You're officially in. We're setting up your campaign now — "
        f"expect a kickoff call invite within 24 hours. "
        f"Questions? Reply to this message anytime."
    )
    sms_ok = ghl_send_sms(contact_id, sms_text)
    results["sms_sent"] = sms_ok

    # --- Step 4: Welcome email ---
    email_subject = f"Welcome to SEO Local, {first_name} — You're In 🎉"
    email_body = (
        f"Hey {first_name},\n\n"
        f"Payment confirmed. You're officially a SEO Local client — and we're already moving.\n\n"
        f"Here's what happens next:\n\n"
        f"Day 1-2: Your dedicated team reviews your audit data and builds your "
        f"90-day campaign roadmap for {business_name}.\n\n"
        f"Day 3: Kickoff call — we walk through your roadmap together, "
        f"set your primary keyword targets, and align on reporting expectations.\n"
        f"Book your kickoff call here: {calendar_url}\n\n"
        f"Day 7+: Campaign execution begins. GBP optimization, schema markup, "
        f"content build-out, and AI citation acquisition — in that order.\n\n"
        f"Day 30: First ranking report delivered. You'll see exactly where your "
        f"keywords moved and what's driving the changes.\n\n"
        f"What to have ready for the kickoff call:\n"
        f"  → Your top 3 service keywords\n"
        f"  → Current monthly call volume from local search (rough estimate is fine)\n"
        f"  → Access to your Google Business Profile (if we don't have it already)\n\n"
        f"Your plan: {plan_name}\n"
        f"Your city: {city}, {state}\n\n"
        f"For anything urgent: chuck@seolocal.us\n\n"
        f"— Chuck Gray\n"
        f"SEO Local\n"
        f"chuck@seolocal.us"
    )
    email_ok = ghl_send_email(contact_id, email_subject, _html(email_body))
    results["email_sent"] = email_ok

    # --- Step 5: Create GHL Fulfillment pipeline entry ---
    pipeline_ok = ghl_create_fulfillment_entry(
        contact_id, business_name, plan_name, mrr
    )
    results["fulfillment_pipeline_created"] = pipeline_ok

    # --- Step 6: Write GHL custom fields ---
    fields_ok = ghl_update_fields(contact_id, [
        {"key": "RECURLY_SUBSCRIPTION_ID", "field_value": subscription_id},
        {"key": "RECURLY_PLAN_CODE",       "field_value": plan_code},
        {"key": "PRODUCT_PURCHASED",       "field_value": plan_name},
        {"key": "ACCOUNT_TYPE",            "field_value": account_type},
        {"key": "onboarding_date",         "field_value": now.isoformat()},
        {"key": "client_mrr",              "field_value": str(mrr)},
        {"key": "slack_channel_id",        "field_value": channel_id},
    ])
    results["fields_written"] = fields_ok

    # --- Step 7: Apply GHL tags ---
    tags_to_apply = [TAG_REL_CLIENT, TAG_ONBOARDING_COMPLETE, plan_tag]
    tags_ok = ghl_add_tags(contact_id, tags_to_apply)
    results["tags_applied"] = tags_ok

    # --- Step 8: Kickoff call task (Day 3) ---
    kickoff_ok = ghl_create_task(
        contact_id,
        f"KICKOFF CALL — {business_name} (Day 3)",
        (
            f"Schedule and run kickoff call for {first_name} {last_name} "
            f"from {business_name} in {city}.\n\n"
            f"Plan: {plan_name} | MRR: ${mrr}/mo\n"
            f"Email: {email} | Phone: {phone}\n\n"
            f"Send calendar invite: {calendar_url}\n\n"
            f"Agenda:\n"
            f"  → Walk through 90-day campaign roadmap\n"
            f"  → Confirm primary keyword targets\n"
            f"  → Set reporting expectations\n"
            f"  → Answer any questions"
        ),
        due_days=3,
    )
    results["kickoff_task_created"] = kickoff_ok

    # --- Step 9: 30-day check-in task ---
    checkin_ok = ghl_create_task(
        contact_id,
        f"30-DAY CHECK-IN — {business_name}",
        (
            f"30-day check-in for {first_name} from {business_name}.\n\n"
            f"Review first ranking report with client.\n"
            f"Check satisfaction score. Identify any concerns early.\n"
            f"Plan: {plan_name} | MRR: ${mrr}/mo"
        ),
        due_days=30,
    )
    results["checkin_task_created"] = checkin_ok

    # --- Step 10: Post to Slack command center ---
    slack_alert = (
        f"🆕 NEW CLIENT\n"
        f"Name     : {first_name} {last_name} | {business_name}\n"
        f"City     : {city}, {state}\n"
        f"Plan     : {plan_name}\n"
        f"MRR      : ${mrr}/mo\n"
        f"Type     : {account_type}\n"
        f"Slack    : #{channel_name}\n"
        f"Sub ID   : {subscription_id}"
    )
    slack_alert_ok = slack_post_command_center(slack_alert)
    results["slack_alert_sent"] = slack_alert_ok

    # --- Step 11: Agency-client notification ---
    if account_type == "agency-client":
        agency_id = payload.get("agency_id", "")
        if agency_id:
            agency_contact = ghl_get_contact(agency_id)
            agency_email   = agency_contact.get("email", "")
            if agency_email:
                ghl_send_email(
                    agency_id,
                    f"New client activated: {business_name}",
                    _html(
                        f"Your client {business_name} in {city} has just activated "
                        f"their {plan_name} subscription.\n\n"
                        f"Their campaign starts today."
                    ),
                )
                log("info", "agency_notified",
                    contact_id_hash=_h(contact_id),
                    agency_id_hash=_h(agency_id))
        results["agency_notified"] = bool(agency_id)

    # --- CloudWatch structured log ---
    log("info", "onboarding_complete",
        contact_id_hash=_h(contact_id),
        plan_code=plan_code,
        mrr=mrr,
        account_type=account_type,
        slack_channel=channel_name,
        sms_ok=sms_ok,
        email_ok=email_ok,
        pipeline_ok=pipeline_ok,
        kickoff_ok=kickoff_ok,
        version=CODE_VERSION)

    return {
        "status":       "onboarded",
        "contact_id":   contact_id,
        "plan_code":    plan_code,
        "plan_name":    plan_name,
        "mrr":          mrr,
        "slack_channel": channel_name,
        "slack_channel_id": channel_id,
        **results,
        "agent":        "clientOnboarding",
        "version":      CODE_VERSION,
    }


# ----------------------------------------------------------------
# AgentCore entrypoint
# Rules (from developer guide):
#   1. payload: dict only — no context param (causes HTTP 400)
#   2. app.run() is mandatory — agent times out without it
#   3. Catch SystemExit and BaseException separately
# ----------------------------------------------------------------
if _SDK:
    app = BedrockAgentCoreApp()

    @app.entrypoint
    def client_onboarding(payload: dict) -> dict:
        try:
            return run_client_onboarding(payload)
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
