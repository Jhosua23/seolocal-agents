"""
================================================================
Agent 23 — Client Comms
================================================================
AgentCore name  : clientComms
Phase           : 2 — Conversion & Retention Engine
Module          : 6 — Client Success & Retention
Wave            : 2
Priority        : P0
Runtime         : Python 3.12 / ARM64
Version         : 1.0.0

NO RDS — all state stored in GHL custom fields + tags.
Retention events logged as GHL tasks + CloudWatch.

3 TRIGGER MODES (routed via payload.action):
  monthly_report  → fires 1st of month via EventBridge cron
                    queries all REL:CLIENT contacts → sends
                    personalised SMS + email per client →
                    90-day QSR check → Slack digest
  churn_check     → GHL webhook on CHURN_RISK_SCORE update
                    score >=7  → Slack alert to Chuck
                    score >=9  → client SMS + CRITICAL Slack
  upsell_detect   → GHL webhook on ROI_CONFIRMED or
                    RANKING_MILESTONE_HIT field update
                    → apply upsell tag → Slack signal

================================================================
SSM Parameters:
  Already in your SSM:
    GHL_API_KEY                   - SecureString
    GHL_DEFAULT_ASSIGNEE_ID       - String
    GHL_LOCATION_ID               - String

  Ask Chuck / Gaurav to add:
    GHL_PIPELINE_FULFILLMENT_ID   - String
    GHL_STAGE_FULFILLMENT_ACTIVE  - String
    SENDGRID_API_KEY              - SecureString
    SENDGRID_FROM_DOMAIN          - String (e.g. mail.seolocal.us)
    SLACK_BOT_TOKEN               - SecureString
    SLACK_CHANNEL_COMMAND_CENTER  - String
    SLACK_CHUCK_USER_ID           - String
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
        "agent":   "clientComms",
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

TAG_REL_CLIENT      = "REL:CLIENT"
TAG_UPSELL_ELIGIBLE = "upsell-eligible"
TAG_UPSELL_3_PAGE1  = "upsell-3-page1"

CHURN_ALERT_THRESHOLD    = 7
CHURN_CRITICAL_THRESHOLD = 9

# Upsell plan recommendations per current plan
UPSELL_MAP = {
    "seolocal-starter":  "seolocal-pro",
    "seolocal-pro":      "seolocal-pro-plus",
    "seolocal-pro-plus": "seolocal-premiere",
    "seolocal-premiere": "seolocal-elite",
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
                method, url, headers=headers, timeout=15, **kwargs
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
# GHL — Get all active clients with pagination
# ----------------------------------------------------------------
def ghl_get_all_active_clients() -> list:
    """
    Fetch all contacts tagged REL:CLIENT in the Fulfillment
    pipeline Active stage. GHL pagination — 100 per page.
    """
    pipeline_id = get_ssm("GHL_PIPELINE_FULFILLMENT_ID", "")
    location_id = get_ssm("GHL_LOCATION_ID", "uXRl9WpDjS7LFjeYfQqD")
    all_contacts = []
    page = 1

    while True:
        try:
            resp = _ghl(
                "GET",
                f"{GHL_BASE}/contacts/",
                params={
                    "locationId": location_id,
                    "query":      f"pipeline:{pipeline_id}" if pipeline_id else "",
                    "tags":       TAG_REL_CLIENT,
                    "limit":      100,
                    "startAfter": (page - 1) * 100,
                },
            )
            if resp.status_code != 200:
                log("warning", "ghl_get_clients_page_failed",
                    page=page, status=resp.status_code)
                break

            contacts = resp.json().get("contacts", [])
            all_contacts.extend(contacts)
            log("info", "clients_page_fetched",
                page=page, count=len(contacts))

            if len(contacts) < 100:
                break
            page += 1

        except Exception as exc:
            log("error", "ghl_pagination_exception",
                page=page, error=str(exc))
            break

    return all_contacts


# ----------------------------------------------------------------
# GHL — Get single contact
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
# GHL — Send email (direct or white-label via SendGrid)
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
# SendGrid — white-label email relay for agency-client accounts
# REST via requests — not gRPC
# ----------------------------------------------------------------
def sendgrid_send_email(
    to_email: str,
    subject: str,
    html_body: str,
    from_email: str,
    from_name: str,
) -> bool:
    api_key = get_ssm("SENDGRID_API_KEY", "")
    if not api_key:
        log("warning", "sendgrid_key_missing",
            note="SENDGRID_API_KEY not in SSM — falling back to GHL email")
        return False
    try:
        resp = requests.post(
            "https://api.sendgrid.com/v3/mail/send",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type":  "application/json",
            },
            json={
                "personalizations": [{"to": [{"email": to_email}]}],
                "from":            {"email": from_email, "name": from_name},
                "subject":         subject,
                "content":         [{"type": "text/html", "value": html_body}],
            },
            timeout=15,
        )
        ok = resp.status_code == 202
        if not ok:
            log("warning", "sendgrid_send_failed",
                status=resp.status_code, body=resp.text[:200])
        return ok
    except Exception as exc:
        log("error", "sendgrid_exception", error=str(exc))
        return False


# ----------------------------------------------------------------
# GHL — Create QSR task (90-day review)
# ----------------------------------------------------------------
def ghl_create_qsr_task(contact_id: str, business_name: str) -> bool:
    resp = _ghl(
        "POST",
        f"{GHL_BASE}/contacts/{contact_id}/tasks",
        json={
            "title":      f"QSR CALL — {business_name} (90-Day Review)",
            "body": (
                f"Schedule Quarterly Strategy Review for {business_name}.\n\n"
                f"Agenda:\n"
                f"  → Review 90-day ranking results\n"
                f"  → Confirm client satisfaction\n"
                f"  → Present Year 2 roadmap\n"
                f"  → Identify upsell opportunity"
            ),
            "assignedTo": get_ssm("GHL_DEFAULT_ASSIGNEE_ID"),
            "contactId":  contact_id,
            "dueDate":    (datetime.now(timezone.utc) + timedelta(days=7)).isoformat() + "Z",
            "completed":  False,
        },
    )
    ok = resp.status_code in (200, 201)
    if not ok:
        log("warning", "ghl_qsr_task_failed",
            contact_id_hash=_h(contact_id), status=resp.status_code)
    return ok


# ----------------------------------------------------------------
# Slack helper — REST via requests (not gRPC)
# ----------------------------------------------------------------
def _slack(endpoint: str, payload: dict) -> dict:
    token = get_ssm("SLACK_BOT_TOKEN", "")
    if not token:
        log("warning", "slack_token_missing")
        return {"ok": False}
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
            log("warning", "slack_error",
                endpoint=endpoint, error=data.get("error", "unknown"))
        return data
    except Exception as exc:
        log("error", "slack_exception", endpoint=endpoint, error=str(exc))
        return {"ok": False}


def slack_post(message: str) -> bool:
    channel = get_ssm("SLACK_CHANNEL_COMMAND_CENTER", "")
    if not channel:
        log("warning", "slack_channel_not_configured")
        return False
    result = _slack("chat.postMessage", {"channel": channel, "text": message})
    return result.get("ok", False)


def slack_dm_chuck(message: str) -> bool:
    chuck_id = get_ssm("SLACK_CHUCK_USER_ID", "")
    if not chuck_id:
        log("warning", "slack_chuck_id_not_configured")
        return False
    result = _slack("chat.postMessage", {"channel": chuck_id, "text": message})
    return result.get("ok", False)


# ----------------------------------------------------------------
# HTML builder
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


def _get_field(contact: dict, key: str) -> str:
    for f in contact.get("customFields", []):
        if f.get("key") == key:
            return str(f.get("value") or "")
    return ""


def _is_near_day_90(onboarding_date_str: str) -> bool:
    """Returns True if client is within 5 days of their 90-day anniversary."""
    if not onboarding_date_str:
        return False
    try:
        onb = datetime.fromisoformat(onboarding_date_str.replace("Z", "+00:00"))
        day_90 = onb + timedelta(days=90)
        now    = datetime.now(timezone.utc)
        diff   = abs((day_90 - now).days)
        return diff <= 5
    except Exception:
        return False


def _next_upsell(plan_code: str) -> str:
    return UPSELL_MAP.get(plan_code, "seolocal-elite")


# ----------------------------------------------------------------
# Build monthly report email per client
# ----------------------------------------------------------------
def build_monthly_report_email(
    contact: dict,
    month_str: str,
) -> tuple:
    """
    Returns (subject, html_body) personalised per client.
    Ranking data sourced from GHL custom fields (no RDS).
    """
    first_name    = contact.get("firstName", "there")
    business_name = contact.get("companyName") or contact.get("firstName", "your business")
    city          = _get_field(contact, "city") or "your area"
    primary_kw    = _get_field(contact, "primary_keyword") or "your primary keyword"
    plan_code     = _get_field(contact, "RECURLY_PLAN_CODE") or "seolocal-pro"

    # Ranking snapshot from GHL custom fields (written by Agent 28)
    kw_rank_now   = _get_field(contact, "current_rank_primary") or "checking"
    kw_rank_prev  = _get_field(contact, "previous_rank_primary") or "—"
    gbp_score     = _get_field(contact, "gbp_health_score") or "—"
    page1_count   = _get_field(contact, "page1_keyword_count") or "0"
    calendar_url  = get_ssm(
        "GHL_CALENDAR_BOOKING_URL",
        "https://api.leadconnectorhq.com/widget/booking/rByTO30SgbyXG7uLMDwV"
    )

    subject = f"Your SEO Local Results — {month_str} | {business_name}"

    body = (
        f"Hey {first_name},\n\n"
        f"Here's your {month_str} ranking update for {business_name}.\n\n"
        f"PRIMARY KEYWORD: {primary_kw}\n"
        f"  Last month  : #{kw_rank_prev}\n"
        f"  This month  : #{kw_rank_now}\n\n"
        f"GBP HEALTH SCORE : {gbp_score}/100\n"
        f"PAGE 1 KEYWORDS  : {page1_count} keywords on Page 1\n\n"
        f"Rankings move as we build more signals — GBP optimisation, "
        f"schema markup, and content authority compound over time. "
        f"You typically see the strongest movement between months 2 and 4.\n\n"
        f"Any questions about your results or what's coming next month? "
        f"Reply here or book a quick call:\n"
        f"{calendar_url}\n\n"
        f"— Chuck Gray\n"
        f"SEO Local\n"
        f"chuck@seolocal.us"
    )
    return subject, _html(body)


def build_monthly_report_sms(contact: dict, month_str: str) -> str:
    first_name    = contact.get("firstName", "there")
    business_name = contact.get("companyName") or contact.get("firstName", "your business")
    kw_rank_now   = _get_field(contact, "current_rank_primary") or "updating"
    primary_kw    = _get_field(contact, "primary_keyword") or "your keyword"
    return (
        f"{first_name} — your {month_str} SEO Local update for {business_name}: "
        f"{primary_kw} is now ranking #{kw_rank_now}. "
        f"Full report in your email. Questions? Reply here."
    )


# ----------------------------------------------------------------
# PATH 1 — MONTHLY REPORT
# ----------------------------------------------------------------
def handle_monthly_report(payload: dict) -> dict:
    now       = datetime.now(timezone.utc)
    month_str = now.strftime("%B %Y")

    log("info", "monthly_report_start", month=month_str)

    clients = ghl_get_all_active_clients()
    total   = len(clients)
    log("info", "active_clients_fetched", total=total)

    if total == 0:
        return {
            "status":       "complete",
            "action":       "monthly_report",
            "month":        month_str,
            "clients_found": 0,
            "reports_sent": 0,
        }

    reports_sent  = 0
    qsr_scheduled = 0
    errors        = 0

    for contact in clients:
        contact_id   = contact.get("id", "")
        account_type = _get_field(contact, "ACCOUNT_TYPE") or "direct"
        email        = contact.get("email", "")
        phone        = contact.get("phone", "")

        if not contact_id or not email:
            continue

        try:
            subj, html = build_monthly_report_email(contact, month_str)
            sms_text   = build_monthly_report_sms(contact, month_str)

            # Agency-client: use SendGrid white-label
            if account_type == "agency-client":
                agency_from  = _get_field(contact, "AGENCY_FROM_EMAIL")
                agency_name  = _get_field(contact, "AGENCY_NAME")
                from_domain  = get_ssm("SENDGRID_FROM_DOMAIN", "mail.seolocal.us")

                if agency_from:
                    sent = sendgrid_send_email(
                        email, subj, html, agency_from, agency_name or "Your SEO Team"
                    )
                else:
                    # Fall back to GHL email if no agency email configured
                    sent = ghl_send_email(contact_id, subj, html)
            else:
                sent = ghl_send_email(contact_id, subj, html)

            if sent:
                reports_sent += 1

            # SMS update
            if phone:
                ghl_send_sms(contact_id, sms_text)

            # 90-day QSR check
            onboarding_date = _get_field(contact, "onboarding_date")
            if _is_near_day_90(onboarding_date):
                business_name = contact.get("companyName") or "Client"
                ghl_create_qsr_task(contact_id, business_name)
                qsr_scheduled += 1
                log("info", "qsr_scheduled",
                    contact_id_hash=_h(contact_id))

        except Exception as exc:
            log("error", "monthly_report_client_error",
                contact_id_hash=_h(contact_id), error=str(exc))
            errors += 1

    # Monthly digest to Slack command center
    slack_msg = (
        f"📊 MONTHLY REPORT — {month_str}\n"
        f"Active clients : {total}\n"
        f"Reports sent   : {reports_sent}\n"
        f"QSR scheduled  : {qsr_scheduled}\n"
        f"Errors         : {errors}"
    )
    slack_post(slack_msg)

    log("info", "monthly_report_complete",
        month=month_str, total=total,
        reports_sent=reports_sent,
        qsr_scheduled=qsr_scheduled,
        errors=errors,
        version=CODE_VERSION)

    return {
        "status":         "complete",
        "action":         "monthly_report",
        "month":          month_str,
        "clients_found":  total,
        "reports_sent":   reports_sent,
        "qsr_scheduled":  qsr_scheduled,
        "errors":         errors,
        "agent":          "clientComms",
        "version":        CODE_VERSION,
    }


# ----------------------------------------------------------------
# PATH 2 — CHURN CHECK
# ----------------------------------------------------------------
def handle_churn_check(payload: dict) -> dict:
    contact_id       = (payload.get("contact_id") or "").strip()
    churn_risk_score = int(payload.get("churn_risk_score") or 0)

    if not contact_id:
        return {"status": "error", "code": "MISSING_CONTACT_ID"}

    log("info", "churn_check",
        contact_id_hash=_h(contact_id),
        score=churn_risk_score)

    contact       = ghl_get_contact(contact_id)
    first_name    = contact.get("firstName", "there")
    business_name = contact.get("companyName") or contact.get("firstName", "Client")
    plan_code     = _get_field(contact, "RECURLY_PLAN_CODE") or "seolocal-pro"
    phone         = contact.get("phone", "")

    sms_sent   = False
    slack_sent = False

    if churn_risk_score >= CHURN_CRITICAL_THRESHOLD:
        # Score >=9 — immediate client outreach + CRITICAL Slack
        if phone:
            sms_text = (
                f"Hi {first_name}, Chuck here at SEO Local. "
                f"I wanted to check in personally on {business_name}'s campaign — "
                f"let's make sure everything is tracking the way you expected. "
                f"Reply here or call me directly anytime."
            )
            sms_sent = ghl_send_sms(contact_id, sms_text)

        slack_msg = (
            f"🚨 CRITICAL CHURN RISK\n"
            f"Name   : {first_name} | {business_name}\n"
            f"Score  : {churn_risk_score}/10\n"
            f"Plan   : {plan_code}\n"
            f"Action : Personal outreach SMS sent — follow up immediately"
        )
        slack_post(slack_msg)
        slack_dm_chuck(slack_msg)
        slack_sent = True

        log("warning", "churn_critical",
            contact_id_hash=_h(contact_id),
            score=churn_risk_score,
            sms_sent=sms_sent)

    elif churn_risk_score >= CHURN_ALERT_THRESHOLD:
        # Score >=7 — Slack alert to Chuck only
        next_plan = _next_upsell(plan_code)
        slack_msg = (
            f"⚠️ CHURN RISK\n"
            f"Name       : {first_name} | {business_name}\n"
            f"Score      : {churn_risk_score}/10\n"
            f"Plan       : {plan_code}\n"
            f"Recommend  : Proactive check-in call. "
            f"Consider offering a service review or early upsell to {next_plan}."
        )
        slack_post(slack_msg)
        slack_dm_chuck(slack_msg)
        slack_sent = True

        log("warning", "churn_alert",
            contact_id_hash=_h(contact_id),
            score=churn_risk_score)

    # Log churn event to GHL custom field
    ghl_add_tags(contact_id, [f"churn-risk-{churn_risk_score}"])

    return {
        "status":         "processed",
        "action":         "churn_check",
        "contact_id":     contact_id,
        "score":          churn_risk_score,
        "sms_sent":       sms_sent,
        "slack_sent":     slack_sent,
        "agent":          "clientComms",
        "version":        CODE_VERSION,
    }


# ----------------------------------------------------------------
# PATH 3 — UPSELL DETECT
# ----------------------------------------------------------------
def handle_upsell_detect(payload: dict) -> dict:
    contact_id          = (payload.get("contact_id") or "").strip()
    roi_confirmed       = payload.get("roi_confirmed", False)
    milestone_hit       = payload.get("ranking_milestone_hit", False)

    if not contact_id:
        return {"status": "error", "code": "MISSING_CONTACT_ID"}

    log("info", "upsell_detect",
        contact_id_hash=_h(contact_id),
        roi_confirmed=roi_confirmed,
        milestone_hit=milestone_hit)

    contact       = ghl_get_contact(contact_id)
    first_name    = contact.get("firstName", "there")
    business_name = contact.get("companyName") or contact.get("firstName", "Client")
    plan_code     = _get_field(contact, "RECURLY_PLAN_CODE") or "seolocal-pro"
    next_plan     = _next_upsell(plan_code)
    page1_count   = _get_field(contact, "page1_keyword_count") or "0"

    tags_applied  = []
    slack_msgs    = []

    if roi_confirmed:
        ghl_add_tags(contact_id, [TAG_UPSELL_ELIGIBLE])
        tags_applied.append(TAG_UPSELL_ELIGIBLE)
        slack_msgs.append(
            f"💰 UPSELL SIGNAL — ROI CONFIRMED\n"
            f"Name      : {first_name} | {business_name}\n"
            f"Plan      : {plan_code}\n"
            f"Recommend : Upgrade to {next_plan}"
        )
        log("info", "upsell_roi_confirmed",
            contact_id_hash=_h(contact_id))

    if milestone_hit:
        ghl_add_tags(contact_id, [TAG_UPSELL_3_PAGE1])
        tags_applied.append(TAG_UPSELL_3_PAGE1)
        slack_msgs.append(
            f"🏆 UPSELL SIGNAL — 3+ PAGE 1 KEYWORDS\n"
            f"Name      : {first_name} | {business_name}\n"
            f"Page 1    : {page1_count} keywords on Page 1\n"
            f"Plan      : {plan_code}\n"
            f"Recommend : Present results + upgrade to {next_plan}"
        )
        log("info", "upsell_milestone_hit",
            contact_id_hash=_h(contact_id),
            page1_count=page1_count)

    for msg in slack_msgs:
        slack_post(msg)
        slack_dm_chuck(msg)

    return {
        "status":        "processed",
        "action":        "upsell_detect",
        "contact_id":    contact_id,
        "tags_applied":  tags_applied,
        "slack_alerts":  len(slack_msgs),
        "agent":         "clientComms",
        "version":       CODE_VERSION,
    }


# ----------------------------------------------------------------
# CORE ROUTER — dispatches on payload.action
# ----------------------------------------------------------------
def run_client_comms(payload: dict) -> dict:
    action = (payload.get("action") or "monthly_report").lower().strip()

    if action == "churn_check":
        return handle_churn_check(payload)
    if action == "upsell_detect":
        return handle_upsell_detect(payload)
    return handle_monthly_report(payload)


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
    def client_comms(payload: dict) -> dict:
        try:
            return run_client_comms(payload)
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
