"""
================================================================
Agent 08 — Lead Nurture Sequencer
================================================================
AgentCore name  : leadNurtureSequencer
Phase           : 1 - Lead Gen & Management
Sequence        : 8 of 15
Priority        : P0
Runtime         : Python 3.12 / ARM64
Version         : 1.0.0

NO RDS — GHL is the single source of truth.
Dedup   : tag  nurture-enrolled  on contact
State   : tags + custom fields on GHL contact
================================================================

SSM Parameters required (all already in your AWS SSM):
  GHL_API_KEY              - SecureString
  GHL_PIPELINE_ID          - String  (xEK3qOXCRezO6aoHN6AS)
  GHL_STAGE_CLOSED_LOST    - String  (be66e86c-1e88-4f70-9f32-569d28ee90ce)
  GHL_SEQUENCE_NURTURE_ID  - String  (GHL workflow ID — ask Chuck)
  GHL_DEFAULT_ASSIGNEE_ID  - String  (4yPiyMcdXbdTUh0Om8m2jp — Jay Leonard)
  GHL_CALENDAR_ID          - String  (GHL native booking calendar ID)

Trigger  : GHL Webhook POST  action=start  (new audit-completed contact)
           GHL Webhook POST  action=stop   (prospect booked)
           EventBridge POST  action=close  (Day 7 no-booking)
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
        "level":  level,
        "event":  event,
        "ts":     datetime.now(timezone.utc).isoformat(),
        "agent":  "leadNurtureSequencer",
        "v":      "1.0.0",
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

# Tags used by this agent — defined once to avoid typos
TAG_NURTURE_ENROLLED  = "nurture-enrolled"
TAG_NURTURE_CONVERTED = "nurture-converted"
TAG_COLD_LEAD         = "cold-lead"
TAG_DEMO_BOOKED       = "demo-booked"
TAG_AUDIT_COMPLETED   = "audit-completed"


# ----------------------------------------------------------------
# SSM — cached, with NextToken pagination (mandatory per dev guide)
# ----------------------------------------------------------------
_ssm_cache: dict = {}


def _ssm_client():
    return boto3.client("ssm", region_name=REGION)


def get_ssm(name: str) -> str:
    """Fetch a single SSM SecureString parameter with in-memory cache."""
    if name in _ssm_cache:
        return _ssm_cache[name]
    val = _ssm_client().get_parameter(
        Name=name, WithDecryption=True
    )["Parameter"]["Value"]
    _ssm_cache[name] = val
    return val


def load_ssm_path(path: str) -> dict:
    """
    Load all SSM parameters under path.
    NextToken loop is mandatory — SSM returns max 10 per page.
    Ref: AgentCore Developer Guide — Known Platform Constraints.
    """
    client = _ssm_client()
    params: dict = {}
    kwargs: dict = {"Path": path, "WithDecryption": True, "Recursive": True}
    while True:
        resp = client.get_parameters_by_path(**kwargs)
        for p in resp.get("Parameters", []):
            key = p["Name"].split("/")[-1]
            params[key] = p["Value"]
            _ssm_cache[key] = p["Value"]
        token = resp.get("NextToken")
        if not token:
            break
        kwargs["NextToken"] = token
    return params


# ----------------------------------------------------------------
# GHL HTTP helper
# Handles: 429 rate limit, 5xx server errors, 401 auth failure
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
            log("warning", "ghl_rate_limited", url=url, attempt=attempt)
            time.sleep(10)
            continue

        if resp.status_code >= 500:
            log("warning", "ghl_server_error",
                status=resp.status_code, url=url, attempt=attempt)
            if attempt == retries:
                raise RuntimeError(f"GHL 5xx error: {resp.status_code}")
            time.sleep(2 ** attempt)
            continue

        return resp

    raise RuntimeError(f"GHL request failed after {retries + 1} attempts: {url}")


# ----------------------------------------------------------------
# GHL — Contact read
# ----------------------------------------------------------------
def ghl_get_contact(contact_id: str) -> dict:
    resp = _ghl("GET", f"{GHL_BASE}/contacts/{contact_id}")
    if resp.status_code == 200:
        return resp.json().get("contact", {})
    log("warning", "ghl_get_contact_failed",
        contact_id_hash=_h(contact_id), status=resp.status_code)
    return {}


# ----------------------------------------------------------------
# GHL — Tags
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
            contact_id_hash=_h(contact_id), tags=tags, status=resp.status_code)
    return ok


def ghl_remove_tags(contact_id: str, tags: list) -> bool:
    resp = _ghl(
        "DELETE",
        f"{GHL_BASE}/contacts/{contact_id}/tags",
        json={"tags": tags},
    )
    ok = resp.status_code in (200, 201)
    if not ok:
        log("warning", "ghl_remove_tags_failed",
            contact_id_hash=_h(contact_id), tags=tags, status=resp.status_code)
    return ok


# ----------------------------------------------------------------
# GHL — Custom fields
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
# GHL — Opportunity  (required for stage transitions)
# ----------------------------------------------------------------
def ghl_get_or_create_opportunity(contact_id: str, business_name: str) -> str | None:
    pipeline_id = get_ssm("GHL_PIPELINE_ID")

    search = _ghl(
        "GET",
        f"{GHL_BASE}/opportunities/search",
        params={"contact_id": contact_id, "pipeline_id": pipeline_id},
    )
    opps = search.json().get("opportunities", []) if search.status_code == 200 else []
    if opps:
        return opps[0]["id"]

    resp = _ghl(
        "POST",
        f"{GHL_BASE}/opportunities/",
        json={
            "pipelineId":      pipeline_id,
            "pipelineStageId": get_ssm("GHL_STAGE_CLOSED_LOST"),
            "contactId":       contact_id,
            "name":            f"{business_name} — SEO Local",
            "status":          "open",
            "monetaryValue":   797,
        },
    )
    if resp.status_code in (200, 201):
        return resp.json().get("opportunity", {}).get("id")
    log("warning", "ghl_create_opportunity_failed",
        contact_id_hash=_h(contact_id), status=resp.status_code)
    return None


def ghl_move_stage(opp_id: str, stage_id: str, status: str = "lost") -> bool:
    resp = _ghl(
        "PUT",
        f"{GHL_BASE}/opportunities/{opp_id}",
        json={
            "pipelineId":      get_ssm("GHL_PIPELINE_ID"),
            "pipelineStageId": stage_id,
            "status":          status,
        },
    )
    ok = resp.status_code in (200, 201)
    if not ok:
        log("warning", "ghl_move_stage_failed",
            opp_id=opp_id, stage_id=stage_id, status=resp.status_code)
    return ok


# ----------------------------------------------------------------
# GHL — Sequence enrollment  (Option A — preferred)
# ----------------------------------------------------------------
def ghl_enroll_sequence(contact_id: str, sequence_id: str) -> bool:
    resp = _ghl(
        "POST",
        f"{GHL_BASE}/contacts/{contact_id}/workflow/{sequence_id}",
        json={},
    )
    ok = resp.status_code in (200, 201)
    if not ok:
        log("warning", "ghl_enroll_sequence_failed",
            contact_id_hash=_h(contact_id),
            sequence_id=sequence_id,
            status=resp.status_code,
            body=resp.text[:300])
    return ok


# ----------------------------------------------------------------
# GHL — Direct email  (Option B fallback)
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
# GHL — Direct SMS  (Option B fallback)
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
# GHL — Urgent internal task  (alert to Jay Leonard)
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
            "dueDate":    (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat() + "Z",
            "completed":  False,
        },
    )
    ok = resp.status_code in (200, 201)
    if not ok:
        log("warning", "ghl_create_task_failed",
            contact_id_hash=_h(contact_id), status=resp.status_code)
    return ok


# ----------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------
def _h(contact_id: str) -> str:
    """Hash contact_id for safe CloudWatch logging — no PII."""
    return hashlib.sha256(contact_id.encode()).hexdigest()[:16]


def _get_custom_field(contact: dict, key: str) -> str:
    """Extract a custom field value from GHL contact object."""
    for f in contact.get("customFields", []):
        if f.get("key") == key:
            return f.get("value") or ""
    return ""


# ----------------------------------------------------------------
# Sequence mode — window-adaptive per spec
# ----------------------------------------------------------------
def get_sequence_mode(hours_since_audit: float) -> str:
    if hours_since_audit <= 6:
        return "full_7day"
    elif hours_since_audit <= 48:
        return "compressed_3"
    elif hours_since_audit <= 72:
        return "compressed_2"
    else:
        return "single_reengagement"


def get_touch_count(mode: str) -> int:
    return {
        "full_7day":           4,
        "compressed_3":        3,
        "compressed_2":        2,
        "single_reengagement": 1,
    }[mode]


def get_schedule(mode: str, now: datetime) -> dict:
    if mode == "full_7day":
        return {
            "t1": now,
            "t2": now + timedelta(hours=24),
            "t3": now + timedelta(hours=48),
            "t4": now + timedelta(days=7),
        }
    elif mode == "compressed_3":
        return {
            "t1": now,
            "t2": now + timedelta(hours=12),
            "t3": now + timedelta(hours=36),
            "t4": None,
        }
    elif mode == "compressed_2":
        return {
            "t1": now,
            "t2": now + timedelta(hours=12),
            "t3": None,
            "t4": None,
        }
    else:  # single_reengagement
        return {"t1": now, "t2": None, "t3": None, "t4": None}


# ----------------------------------------------------------------
# Personalization variable builder
# ----------------------------------------------------------------
_FALLBACK_ISSUE = (
    "Your website has critical SEO gaps that are costing you local customers every day."
)


def build_vars(payload: dict, contact: dict) -> dict:
    first_name    = (payload.get("first_name") or contact.get("firstName") or "there").strip()
    business_name = (
        payload.get("business_name") or contact.get("companyName") or "your business"
    ).strip()
    score  = int(payload.get("audit_score") or _get_custom_field(contact, "audit_score") or 0)
    grade  = (payload.get("audit_grade")  or _get_custom_field(contact, "audit_grade")  or "D").strip().upper()
    issue1 = (payload.get("audit_top_issue_1") or _get_custom_field(contact, "audit_top_issue_1") or _FALLBACK_ISSUE).strip()
    issue2 = (payload.get("audit_top_issue_2") or _get_custom_field(contact, "audit_top_issue_2") or _FALLBACK_ISSUE).strip()
    issue3 = (payload.get("audit_top_issue_3") or _get_custom_field(contact, "audit_top_issue_3") or _FALLBACK_ISSUE).strip()
    city     = (payload.get("city")     or "your area").strip()
    vertical = (payload.get("vertical") or "local business").strip()

    return {
        "first_name":    first_name,
        "business_name": business_name,
        "score":         score,
        "grade":         grade,
        "issue1":        issue1,
        "issue1_short":  " ".join(issue1.split()[:6]),
        "issue2":        issue2,
        "issue3":        issue3,
        "city":          city,
        "vertical":      vertical,
        "calendly":      get_ssm("GHL_CALENDAR_ID"),
    }


# ----------------------------------------------------------------
# HTML wrapper for GHL email delivery
# ----------------------------------------------------------------
def _html(plain: str) -> str:
    lines = plain.strip().split("\n")
    body  = "".join(
        f"<p style='margin:0 0 12px 0'>{ln if ln.strip() else '&nbsp;'}</p>"
        for ln in lines
    )
    return (
        "<div style='font-family:Arial,Helvetica,sans-serif;"
        "font-size:15px;line-height:1.6;color:#1a1a1a;"
        "max-width:600px;margin:0 auto'>"
        f"{body}</div>"
    )


# ----------------------------------------------------------------
# Email + SMS builders  (one function per touch)
# ----------------------------------------------------------------
def touch_1_email(v: dict) -> tuple:
    subj = f"Your website scored a {v['grade']} — here's what's costing {v['business_name']} customers"
    body = (
        f"Hey {v['first_name']},\n\n"
        f"I just finished a 13-point audit on {v['business_name']}'s website.\n\n"
        f"Your score: {v['score']}/130  |  Grade: {v['grade']}\n\n"
        f"The #1 issue holding you back:\n"
        f"{v['issue1']}\n\n"
        f"This alone is likely costing you 3–5 leads per month in {v['city']}.\n\n"
        f"I'd like to walk you through the full breakdown — what each issue means, "
        f"what to fix first, and what it would take to get you ranking above your competitors.\n\n"
        f"It's a free 15-minute call. No pitch. Just the data.\n\n"
        f"Book a time here: {v['calendly']}\n\n"
        f"— Mike\n"
        f"SEO Local"
    )
    return subj, _html(body)


def touch_2_email(v: dict, is_final: bool = False) -> tuple:
    if is_final:
        subj = f"Still here if the timing is right, {v['first_name']}"
        body = (
            f"Hey {v['first_name']},\n\n"
            f"One more note about {v['business_name']}.\n\n"
            f"Your website scored {v['score']}/130 — and the issues we found "
            f"aren't going away on their own.\n\n"
            f"If the timing is right, I'd love to show you exactly what to fix first.\n\n"
            f"Book here when you're ready: {v['calendly']}\n\n"
            f"— Mike\n"
            f"SEO Local"
        )
    else:
        subj = "The one fix that would move your score the most"
        body = (
            f"Hey {v['first_name']},\n\n"
            f"Yesterday I mentioned {v['business_name']}'s audit score of {v['score']}/130.\n\n"
            f"Here's your second biggest issue:\n"
            f"{v['issue2']}\n\n"
            f"For {v['vertical']} businesses in {v['city']}, this typically means "
            f"4–6 missed service calls per month. Every month you wait, "
            f"that's revenue going to your competitors.\n\n"
            f"The fix isn't complicated — but it does need to happen.\n\n"
            f"15 minutes on a call and I'll show you exactly what to do:\n"
            f"{v['calendly']}\n\n"
            f"— Mike\n"
            f"SEO Local"
        )
    return subj, _html(body)


def touch_2_sms(v: dict, is_final: bool = False) -> str:
    if is_final:
        return (
            f"{v['first_name']} — still here if the timing is right. "
            f"{v['business_name']} scored {v['score']}/130. "
            f"{v['calendly']} — Mike at SEO Local"
        )
    return (
        f"Hey {v['first_name']}, Mike at SEO Local. "
        f"Your site scored {v['score']}/130 — "
        f"that {v['issue1_short']} is costing you calls. "
        f"15 min to see the fix? {v['calendly']}"
    )


def touch_3_email(v: dict, is_final: bool = False) -> tuple:
    if is_final:
        subj = f"One last thing about {v['business_name']}'s score"
        body = (
            f"Hey {v['first_name']},\n\n"
            f"Last message from me this week.\n\n"
            f"{v['business_name']} scored {v['score']}/130 on our audit. "
            f"Your third issue:\n"
            f"{v['issue3']}\n\n"
            f"If you're ready to fix it, here's my calendar:\n"
            f"{v['calendly']}\n\n"
            f"— Mike\n"
            f"SEO Local"
        )
    else:
        subj = f"{v['business_name']}: your competitors are eating your lunch right now"
        body = (
            f"Hey {v['first_name']},\n\n"
            f"I'll be direct.\n\n"
            f"While {v['business_name']} sits at {v['score']}/130, "
            f"your competitors are ranking for the searches that should be yours.\n\n"
            f"Your third issue:\n"
            f"{v['issue3']}\n\n"
            f"I only take 5 free strategy calls per week — two spots left this week.\n\n"
            f"If you want one: {v['calendly']}\n\n"
            f"— Mike\n"
            f"SEO Local"
        )
    return subj, _html(body)


def touch_4_email(v: dict) -> tuple:
    subj = f"Last note from me, {v['first_name']}"
    body = (
        f"Hey {v['first_name']},\n\n"
        f"I've reached out a few times about {v['business_name']}'s "
        f"{v['grade']} website score.\n\n"
        f"I understand timing isn't always right — and that's completely okay.\n\n"
        f"If things change and you're ready to look at what it would take to fix your "
        f"{v['score']}/130 score and start showing up where your customers are searching, "
        f"your audit results will be waiting.\n\n"
        f"Just book a time when you're ready:\n"
        f"{v['calendly']}\n\n"
        f"Wishing you the best either way.\n\n"
        f"— Mike\n"
        f"SEO Local"
    )
    return subj, _html(body)


def touch_4_sms(v: dict) -> str:
    return (
        f"{v['first_name']} — last message from me. "
        f"If you're ready to fix that {v['score']}/130 score, "
        f"here's my calendar: {v['calendly']} — Mike at SEO Local"
    )


# ----------------------------------------------------------------
# Option B — direct GHL API delivery (fallback when sequence not built)
# Sends Touch 1 immediately.
# Touches 2–4 must be scheduled via GHL workflows or EventBridge.
# ----------------------------------------------------------------
def option_b_send(contact_id: str, v: dict, mode: str, has_phone: bool) -> bool:
    log("info", "option_b_start",
        contact_id_hash=_h(contact_id), mode=mode)

    subj, html = touch_1_email(v)
    ok = ghl_send_email(contact_id, subj, html)
    if not ok:
        log("error", "option_b_touch1_email_failed",
            contact_id_hash=_h(contact_id))
        return False

    log("info", "option_b_touch1_sent", contact_id_hash=_h(contact_id))
    log("warning", "option_b_touches_234_need_scheduling",
        contact_id_hash=_h(contact_id),
        note="Touches 2-4 require GHL workflow or EventBridge — not sent by this agent in Option B")
    return True


# ----------------------------------------------------------------
# handle_start  (/nurture/start)
# Main enrollment logic — runs when GHL fires on audit-completed tag
# ----------------------------------------------------------------
def handle_start(payload: dict) -> dict:
    now = datetime.now(timezone.utc)

    # --- Validate required fields ---
    contact_id = (payload.get("contact_id") or "").strip()
    email      = (payload.get("email") or "").strip()
    if not contact_id:
        return {"status": "error", "code": "MISSING_CONTACT_ID"}
    if not email:
        return {"status": "error", "code": "MISSING_EMAIL"}

    log("info", "handle_start_begin", contact_id_hash=_h(contact_id))

    # --- Step 1: Fetch contact and run guards ---
    contact = ghl_get_contact(contact_id)
    if not contact:
        log("error", "contact_not_found", contact_id_hash=_h(contact_id))
        return {"status": "error", "code": "CONTACT_NOT_FOUND"}

    existing_tags = contact.get("tags", [])

    # Guard 1 — already enrolled (dedup via GHL tag)
    if TAG_NURTURE_ENROLLED in existing_tags:
        log("info", "skip_already_enrolled", contact_id_hash=_h(contact_id))
        return {"status": "skipped", "reason": "already_enrolled"}

    # Guard 2 — already booked (no nurture needed)
    if TAG_DEMO_BOOKED in existing_tags:
        log("info", "skip_already_booked", contact_id_hash=_h(contact_id))
        return {"status": "skipped", "reason": "already_booked"}

    business_name = (
        payload.get("business_name") or contact.get("companyName") or "your business"
    ).strip()
    has_phone = bool(
        (payload.get("phone") or contact.get("phone") or "").strip()
    )

    # --- Step 2: Window-adaptive sequence mode ---
    hours_since_audit = 0.0
    audit_date_val = _get_custom_field(contact, "audit_date")
    if audit_date_val:
        try:
            audit_dt = datetime.fromisoformat(audit_date_val.replace("Z", "+00:00"))
            hours_since_audit = (now - audit_dt).total_seconds() / 3600
        except (ValueError, TypeError):
            pass

    mode        = get_sequence_mode(hours_since_audit)
    touch_count = get_touch_count(mode)
    schedule    = get_schedule(mode, now)

    log("info", "mode_selected",
        contact_id_hash=_h(contact_id),
        mode=mode,
        touch_count=touch_count,
        hours_since_audit=round(hours_since_audit, 1))

    # --- Steps 3 & 4: Build personalisation ---
    v = build_vars(payload, contact)

    # --- Step 5: Enroll in GHL sequence (Option A) ---
    sequence_id = get_ssm("GHL_SEQUENCE_NURTURE_ID")
    enrolled    = ghl_enroll_sequence(contact_id, sequence_id)

    # --- Fallback: Option B ---
    if not enrolled:
        log("warning", "option_a_failed_fallback_option_b",
            contact_id_hash=_h(contact_id))
        enrolled = option_b_send(contact_id, v, mode, has_phone)

    # --- Last resort: create urgent GHL task ---
    if not enrolled:
        log("error", "all_enrollment_failed",
            contact_id_hash=_h(contact_id))
        ghl_create_task(
            contact_id,
            "URGENT — Nurture enrollment failed",
            f"Both Option A (GHL sequence) and Option B (direct send) failed "
            f"for {business_name} ({email}). Manual nurture required. Mode: {mode}.",
        )
        return {
            "status": "error",
            "code":   "ENROLLMENT_FAILED",
            "contact_id": contact_id,
        }

    # --- Step 6: Update GHL contact — tag + custom fields ---
    sequence_version = f"v2-{mode}"

    ghl_add_tags(contact_id, [TAG_NURTURE_ENROLLED])

    ghl_update_fields(contact_id, [
        {"key": "nurture_enrolled_at",      "field_value": now.isoformat()},
        {"key": "nurture_sequence_version", "field_value": sequence_version},
        {"key": "nurture_touch_count",      "field_value": str(touch_count)},
        {"key": "nurture_t2_scheduled_at",  "field_value": schedule["t2"].isoformat() if schedule["t2"] else ""},
        {"key": "nurture_t3_scheduled_at",  "field_value": schedule["t3"].isoformat() if schedule["t3"] else ""},
        {"key": "nurture_t4_scheduled_at",  "field_value": schedule["t4"].isoformat() if schedule["t4"] else ""},
    ])

    # --- Step 7: CloudWatch execution log ---
    log("info", "nurture_enrolled",
        contact_id_hash=_h(contact_id),
        mode=mode,
        touch_count=touch_count,
        hours_since_audit=round(hours_since_audit, 1),
        has_phone=has_phone,
        sequence_version=sequence_version,
        version=CODE_VERSION)

    return {
        "status":           "enrolled",
        "contact_id":       contact_id,
        "sequence_mode":    mode,
        "touch_count":      touch_count,
        "enrolled_at":      now.isoformat(),
        "sequence_version": sequence_version,
        "version":          CODE_VERSION,
        "agent":            "leadNurtureSequencer",
    }


# ----------------------------------------------------------------
# handle_stop  (/nurture/stop)
# Fires when prospect books — called by Agent 07 via GHL webhook
# ----------------------------------------------------------------
def handle_stop(payload: dict) -> dict:
    contact_id = (payload.get("contact_id") or "").strip()
    if not contact_id:
        return {"status": "error", "code": "MISSING_CONTACT_ID"}

    log("info", "handle_stop", contact_id_hash=_h(contact_id))

    ghl_remove_tags(contact_id, [TAG_NURTURE_ENROLLED])
    ghl_add_tags(contact_id,    [TAG_NURTURE_CONVERTED])
    ghl_update_fields(contact_id, [
        {"key": "nurture_converted_at", "field_value": datetime.now(timezone.utc).isoformat()},
        {"key": "nurture_outcome",      "field_value": "converted"},
    ])

    log("info", "nurture_stopped", contact_id_hash=_h(contact_id))
    return {
        "status":     "stopped",
        "contact_id": contact_id,
        "agent":      "leadNurtureSequencer",
    }


# ----------------------------------------------------------------
# handle_close  (/nurture/close)
# Fires on Day 7 no-booking — called by EventBridge scheduled rule
# ----------------------------------------------------------------
def handle_close(payload: dict) -> dict:
    contact_id    = (payload.get("contact_id") or "").strip()
    business_name = (payload.get("business_name") or "prospect").strip()
    if not contact_id:
        return {"status": "error", "code": "MISSING_CONTACT_ID"}

    log("info", "handle_close", contact_id_hash=_h(contact_id))

    # Safety check — do not close if they booked during Day 7
    contact = ghl_get_contact(contact_id)
    if TAG_DEMO_BOOKED in contact.get("tags", []):
        log("info", "close_skipped_booked", contact_id_hash=_h(contact_id))
        return {"status": "skipped", "reason": "already_booked"}

    # Move pipeline stage to Closed Lost
    opp_id = ghl_get_or_create_opportunity(contact_id, business_name)
    if opp_id:
        ghl_move_stage(opp_id, get_ssm("GHL_STAGE_CLOSED_LOST"), status="lost")

    # Apply tags
    ghl_add_tags(contact_id,    [TAG_COLD_LEAD])
    ghl_remove_tags(contact_id, [TAG_NURTURE_ENROLLED, TAG_AUDIT_COMPLETED])

    # Write outcome to GHL custom fields
    ghl_update_fields(contact_id, [
        {"key": "nurture_outcome",      "field_value": "closed_lost"},
        {"key": "nurture_closed_at",    "field_value": datetime.now(timezone.utc).isoformat()},
    ])

    log("info", "nurture_closed_lost", contact_id_hash=_h(contact_id))
    return {
        "status":     "closed_lost",
        "contact_id": contact_id,
        "agent":      "leadNurtureSequencer",
    }


# ----------------------------------------------------------------
# Router — dispatches on payload.action
# ----------------------------------------------------------------
def handle(payload: dict) -> dict:
    action = (payload.get("action") or "start").lower().strip()
    if action == "stop":
        return handle_stop(payload)
    if action == "close":
        return handle_close(payload)
    return handle_start(payload)


# ----------------------------------------------------------------
# AgentCore entrypoint
# RULES (from AgentCore developer guide):
#   1. Function signature must be payload: dict only — no context param
#   2. app.run() is mandatory — agent times out without it
#   3. Catch SystemExit and BaseException separately
# ----------------------------------------------------------------
if _SDK:
    app = BedrockAgentCoreApp()

    @app.entrypoint
    def lead_nurture_sequencer(payload: dict) -> dict:
        try:
            return handle(payload)
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

    if __name__ == "__main__":
        app.run()