"""
================================================================
Agent 12 — GHL Pipeline Manager
================================================================
AgentCore name  : ghlPipelineManager
Phase           : 1 - Lead Generation
Sequence        : 12 of 15
Priority        : P1
Runtime         : Python 3.12 / ARM64
Version         : 1.0.0

NO RDS — health report written as GHL task only.
Trigger : EventBridge cron — daily at 7:00 AM EST
================================================================

SSM Parameters (all already in your SSM):
  GHL_API_KEY              - SecureString
  GHL_PIPELINE_ID          - String
  STALE_THRESHOLD_HOURS    - String  (default 48)
  GHL_NOTIFICATION_USER_ID - String  (Jay Leonard)
  GHL_DEFAULT_ASSIGNEE_ID  - String  (Jay Leonard)

Optional (add later):
  AGENT_03_ENDPOINT        - String  (self-heal enrichment)
  AGENT_08_ENDPOINT        - String  (self-heal nurture)
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
# Logging
# ----------------------------------------------------------------
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def log(level: str, event: str, **kwargs) -> None:
    record = {
        "level":   level,
        "event":   event,
        "ts":      datetime.now(timezone.utc).isoformat(),
        "agent":   "ghlPipelineManager",
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

# Health thresholds
STALE_DEFAULT_HOURS      = 48
MISSING_DATA_HOURS       = 4
NO_NURTURE_HOURS         = 72


# ----------------------------------------------------------------
# SSM
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
# GHL HTTP helper
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
# GHL — Get all contacts in pipeline with pagination
# ----------------------------------------------------------------
def ghl_get_all_pipeline_contacts(pipeline_id: str) -> list:
    """
    Fetch all contacts in pipeline using GHL pagination.
    GHL returns max 100 per page — loop until exhausted.
    """
    all_contacts = []
    page = 1
    while True:
        try:
            resp = _ghl(
                "GET",
                f"{GHL_BASE}/contacts/",
                params={
                    "locationId": LOCATION_ID,
                    "query":      f"pipeline:{pipeline_id}",
                    "limit":      100,
                    "startAfter": (page - 1) * 100,
                },
            )
            if resp.status_code != 200:
                log("warning", "ghl_contacts_page_failed",
                    page=page, status=resp.status_code)
                break

            data     = resp.json()
            contacts = data.get("contacts", [])
            all_contacts.extend(contacts)

            log("info", "ghl_contacts_page_fetched",
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
# GHL — Create task
# ----------------------------------------------------------------
def ghl_create_task(contact_id: str, title: str, body: str,
                    hours_due: int = 4) -> bool:
    assignee = get_ssm("GHL_DEFAULT_ASSIGNEE_ID")
    due = (datetime.now(timezone.utc) + timedelta(hours=hours_due)).isoformat() + "Z"
    resp = _ghl(
        "POST",
        f"{GHL_BASE}/contacts/{contact_id}/tasks",
        json={
            "title":      title,
            "body":       body,
            "assignedTo": assignee,
            "contactId":  contact_id,
            "dueDate":    due,
            "completed":  False,
        },
    )
    ok = resp.status_code in (200, 201)
    if not ok:
        log("warning", "ghl_create_task_failed",
            contact_id_hash=_h(contact_id), status=resp.status_code)
    return ok


# ----------------------------------------------------------------
# GHL — Create health notification task (no contact_id needed)
# ----------------------------------------------------------------
def ghl_create_health_notification(report: str) -> bool:
    """
    Creates a GHL task assigned to the notification user
    with the full pipeline health report as the body.
    Uses a dummy contact search or creates standalone task.
    """
    notification_user = get_ssm("GHL_NOTIFICATION_USER_ID")
    today = datetime.now(timezone.utc).strftime("%B %d, %Y")
    due   = (datetime.now(timezone.utc) + timedelta(hours=12)).isoformat() + "Z"

    # Post as a general note to the pipeline manager contact
    # (GHL tasks require contact_id — use notification user as proxy)
    resp = _ghl(
        "POST",
        f"{GHL_BASE}/contacts/search",
        json={
            "locationId": LOCATION_ID,
            "filters": [{"field": "assignedTo", "operator": "eq", "value": notification_user}],
            "limit": 1,
        },
    )
    contacts = resp.json().get("contacts", []) if resp.status_code == 200 else []

    if contacts:
        contact_id = contacts[0]["id"]
        resp2 = _ghl(
            "POST",
            f"{GHL_BASE}/contacts/{contact_id}/tasks",
            json={
                "title":      f"Daily Pipeline Health Report — {today}",
                "body":       report,
                "assignedTo": notification_user,
                "contactId":  contact_id,
                "dueDate":    due,
                "completed":  False,
            },
        )
        return resp2.status_code in (200, 201)

    log("warning", "health_notification_no_contact_found")
    return False


# ----------------------------------------------------------------
# Self-heal — invoke Agent 03 for missing enrichment
# ----------------------------------------------------------------
def self_heal_enrichment(contact: dict) -> bool:
    endpoint = get_ssm("AGENT_03_ENDPOINT", "")
    if not endpoint:
        log("info", "self_heal_enrichment_skipped",
            reason="AGENT_03_ENDPOINT not set in SSM")
        return False
    try:
        resp = requests.post(
            endpoint,
            json={
                "contact_id":    contact.get("id"),
                "email":         contact.get("email", ""),
                "business_name": contact.get("companyName", ""),
                "city":          _get_field(contact, "city"),
            },
            timeout=30,
        )
        ok = resp.status_code == 200
        log("info" if ok else "warning", "self_heal_enrichment_result",
            contact_id_hash=_h(contact.get("id", "")),
            status=resp.status_code)
        return ok
    except Exception as exc:
        log("error", "self_heal_enrichment_exception", error=str(exc))
        return False


# ----------------------------------------------------------------
# Self-heal — invoke Agent 08 for missing nurture
# ----------------------------------------------------------------
def self_heal_nurture(contact: dict) -> bool:
    endpoint = get_ssm("AGENT_08_ENDPOINT", "")
    if not endpoint:
        log("info", "self_heal_nurture_skipped",
            reason="AGENT_08_ENDPOINT not set in SSM")
        return False
    try:
        resp = requests.post(
            endpoint,
            json={
                "action":        "start",
                "contact_id":    contact.get("id"),
                "email":         contact.get("email", ""),
                "business_name": contact.get("companyName", ""),
                "audit_score":   _get_field(contact, "audit_score"),
                "audit_grade":   _get_field(contact, "audit_grade"),
            },
            timeout=30,
        )
        ok = resp.status_code == 200
        log("info" if ok else "warning", "self_heal_nurture_result",
            contact_id_hash=_h(contact.get("id", "")),
            status=resp.status_code)
        return ok
    except Exception as exc:
        log("error", "self_heal_nurture_exception", error=str(exc))
        return False


# ----------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------
def _h(contact_id: str) -> str:
    return hashlib.sha256(str(contact_id).encode()).hexdigest()[:16]


def _get_field(contact: dict, key: str) -> str:
    for f in contact.get("customFields", []):
        if f.get("key") == key:
            return f.get("value") or ""
    return ""


def _hours_since(dt_str: str) -> float:
    """Return hours elapsed since an ISO datetime string."""
    if not dt_str:
        return 9999.0
    try:
        dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        return (datetime.now(timezone.utc) - dt).total_seconds() / 3600
    except Exception:
        return 9999.0


def _is_stale(contact: dict, threshold_hours: int) -> bool:
    date_added = contact.get("dateAdded") or contact.get("createdAt") or ""
    return _hours_since(date_added) > threshold_hours


def _is_missing_enrichment(contact: dict) -> bool:
    enrichment_date = _get_field(contact, "enrichment_date")
    if enrichment_date:
        return False
    date_added = contact.get("dateAdded") or contact.get("createdAt") or ""
    return _hours_since(date_added) > MISSING_DATA_HOURS


def _is_missing_nurture(contact: dict) -> bool:
    tags = contact.get("tags", [])
    if "nurture-enrolled" in tags or "demo-booked" in tags or "nurture-converted" in tags:
        return False
    if "audit-completed" not in tags:
        return False
    date_added = contact.get("dateAdded") or contact.get("createdAt") or ""
    return _hours_since(date_added) > NO_NURTURE_HOURS


# ----------------------------------------------------------------
# CORE AGENT LOGIC
# ----------------------------------------------------------------
def run_pipeline_manager(payload: dict) -> dict:
    now = datetime.now(timezone.utc)
    today_str = now.strftime("%B %d, %Y")

    log("info", "pipeline_manager_start", date=today_str)

    pipeline_id      = get_ssm("GHL_PIPELINE_ID")
    stale_hours      = int(get_ssm("STALE_THRESHOLD_HOURS", str(STALE_DEFAULT_HOURS)))

    if not pipeline_id:
        return {"status": "error", "code": "MISSING_GHL_PIPELINE_ID"}

    # --- Step 1: Fetch all pipeline contacts ---
    log("info", "fetching_pipeline_contacts")
    contacts = ghl_get_all_pipeline_contacts(pipeline_id)
    total    = len(contacts)
    log("info", "contacts_fetched", total=total)

    if total == 0:
        log("info", "pipeline_empty")
        return {"status": "success", "message": "Pipeline empty — nothing to monitor"}

    # --- Step 2: Categorise contacts ---
    stale_contacts      = []
    missing_enrichment  = []
    missing_nurture     = []
    healthy_contacts    = []

    for contact in contacts:
        tags = contact.get("tags", [])

        # Skip if already closed
        if "cold-lead" in tags or "signed-up" in tags:
            healthy_contacts.append(contact)
            continue

        is_stale     = _is_stale(contact, stale_hours)
        miss_enrich  = _is_missing_enrichment(contact)
        miss_nurture = _is_missing_nurture(contact)

        if miss_enrich:
            missing_enrichment.append(contact)
        elif miss_nurture:
            missing_nurture.append(contact)
        elif is_stale:
            stale_contacts.append(contact)
        else:
            healthy_contacts.append(contact)

    log("info", "categorisation_complete",
        total=total,
        healthy=len(healthy_contacts),
        stale=len(stale_contacts),
        missing_enrichment=len(missing_enrichment),
        missing_nurture=len(missing_nurture))

    # --- Step 3: Handle stale contacts — create GHL tasks ---
    stale_tasks_created = 0
    for contact in stale_contacts:
        contact_id    = contact.get("id", "")
        business_name = contact.get("companyName") or contact.get("firstName", "Unknown")
        date_added    = contact.get("dateAdded") or contact.get("createdAt") or ""
        hours_stalled = round(_hours_since(date_added), 1)

        ok = ghl_create_task(
            contact_id,
            "Stalled lead — manual review needed",
            f"Contact '{business_name}' has been in the pipeline for "
            f"{hours_stalled} hours with no progression.\n\n"
            f"Email: {contact.get('email', 'unknown')}\n"
            f"Tags: {', '.join(contact.get('tags', []))}\n\n"
            f"Action required: Review and manually trigger next step.",
            hours_due=4,
        )
        if ok:
            stale_tasks_created += 1
        log("info", "stale_task_created" if ok else "stale_task_failed",
            contact_id_hash=_h(contact_id), hours_stalled=hours_stalled)

    # --- Step 4: Self-heal missing enrichment ---
    enrichment_healed = 0
    enrichment_failed = 0
    for contact in missing_enrichment:
        healed = self_heal_enrichment(contact)
        if healed:
            enrichment_healed += 1
        else:
            enrichment_failed += 1
            # Create manual task as fallback
            ghl_create_task(
                contact.get("id", ""),
                "Missing enrichment — manual lookup needed",
                f"Contact '{contact.get('companyName', 'Unknown')}' "
                f"({contact.get('email', '')}) has no enrichment data. "
                f"Self-heal failed. Manual GBP lookup required.",
                hours_due=8,
            )

    # --- Step 5 & 6: Self-heal missing nurture ---
    nurture_healed = 0
    nurture_failed = 0
    for contact in missing_nurture:
        healed = self_heal_nurture(contact)
        if healed:
            nurture_healed += 1
        else:
            nurture_failed += 1
            ghl_create_task(
                contact.get("id", ""),
                "Nurture gap — check email sequence",
                f"Contact '{contact.get('companyName', 'Unknown')}' "
                f"({contact.get('email', '')}) has tag 'audit-completed' "
                f"but no nurture sequence enrolled after "
                f"{NO_NURTURE_HOURS}+ hours. Manual nurture required.",
                hours_due=8,
            )

    # --- Step 7: Calculate health score ---
    healthy_pct = round((len(healthy_contacts) / total) * 100, 1) if total else 0
    stale_pct   = round((len(stale_contacts)   / total) * 100, 1) if total else 0

    # --- Step 8: Build health report ---
    report = (
        f"Pipeline Health Report — {today_str}\n"
        f"{'='*50}\n\n"
        f"SUMMARY\n"
        f"  Total leads in pipeline : {total}\n"
        f"  Healthy                 : {len(healthy_contacts)} ({healthy_pct}%)\n"
        f"  Stale (> {stale_hours}hr)         : {len(stale_contacts)} ({stale_pct}%)\n"
        f"  Missing enrichment      : {len(missing_enrichment)}\n"
        f"  Missing nurture         : {len(missing_nurture)}\n\n"
        f"ACTIONS TAKEN\n"
        f"  Stale lead tasks created    : {stale_tasks_created}\n"
        f"  Enrichment self-healed      : {enrichment_healed}\n"
        f"  Enrichment manual tasks     : {enrichment_failed}\n"
        f"  Nurture self-healed         : {nurture_healed}\n"
        f"  Nurture manual tasks        : {nurture_failed}\n\n"
        f"Generated by ghlPipelineManager v{CODE_VERSION} at {now.isoformat()}"
    )

    # --- Step 9: Send daily health notification ---
    log("info", "sending_health_notification")
    ghl_create_health_notification(report)

    # --- Step 10: CloudWatch log ---
    log("info", "pipeline_manager_complete",
        total=total,
        healthy=len(healthy_contacts),
        stale=len(stale_contacts),
        missing_enrichment=len(missing_enrichment),
        missing_nurture=len(missing_nurture),
        stale_tasks_created=stale_tasks_created,
        enrichment_healed=enrichment_healed,
        nurture_healed=nurture_healed,
        version=CODE_VERSION)

    return {
        "status":               "complete",
        "date":                 today_str,
        "total_contacts":       total,
        "healthy":              len(healthy_contacts),
        "stale":                len(stale_contacts),
        "missing_enrichment":   len(missing_enrichment),
        "missing_nurture":      len(missing_nurture),
        "stale_tasks_created":  stale_tasks_created,
        "enrichment_healed":    enrichment_healed,
        "nurture_healed":       nurture_healed,
        "agent":                "ghlPipelineManager",
        "version":              CODE_VERSION,
    }


# ----------------------------------------------------------------
# AgentCore entrypoint
# ----------------------------------------------------------------
if _SDK:
    app = BedrockAgentCoreApp()

    @app.entrypoint
    def ghl_pipeline_manager(payload: dict) -> dict:
        try:
            return run_pipeline_manager(payload)
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
