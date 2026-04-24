"""
Agent 24 — Cold Nurture
Phase 2 — Conversion & Retention Engine
AgentCore ID: coldNurture

Purpose
-------
Enrolls lost / unresponsive leads in a 6-month authority drip sequence the
moment they are tagged MOD:COLD_NURTURE in GHL. Sends zero-pitch value
content on weeks 1, 2, 4, 8, 12, 16, 20, 24. On any hand-raise signal
(tag MOD:HAND_RAISED) or conversion (tag REL:CLIENT) the sequence
immediately stops and remaining EventBridge rules are cleaned up.

Endpoints
---------
POST /nurture/cold/enroll   -> enrolls new contact + schedules 8 rules
POST /nurture/cold/send     -> called by EventBridge on each cadence

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
log = logging.getLogger("coldNurture")

app = BedrockAgentCoreApp()

# ---------------------------------------------------------------------------
# AWS clients
# ---------------------------------------------------------------------------
REGION = os.environ.get("AWS_REGION", "us-east-1")
_ssm = boto3.client("ssm", region_name=REGION)
_events = boto3.client("events", region_name=REGION)
_lambda = boto3.client("lambda", region_name=REGION)

# ---------------------------------------------------------------------------
# SSM cache
# ---------------------------------------------------------------------------
_SSM_CACHE: dict = {}


def get_ssm(name: str, required: bool = True, default: str = "") -> str:
    """Fetch SSM parameter with in-memory cache. Supports SecureString."""
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
# Cadence schedule (in days from enrollment)
# ---------------------------------------------------------------------------
CADENCE_DAYS = [7, 14, 28, 56, 84, 112, 140, 168]  # 8 touches over 24 weeks

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


def ghl_get_contact(contact_id: str) -> dict:
    url = f"{GHL_BASE}/contacts/{contact_id}"
    r = requests.get(url, headers=_ghl_headers(), timeout=20)
    if r.status_code == 404:
        return {}
    r.raise_for_status()
    return r.json().get("contact", {})


def ghl_contact_has_tag(contact: dict, tag: str) -> bool:
    tags = contact.get("tags") or []
    tag_lower = tag.lower()
    return any((t or "").lower() == tag_lower for t in tags)


def ghl_add_tag(contact_id: str, tag: str) -> bool:
    url = f"{GHL_BASE}/contacts/{contact_id}/tags"
    r = requests.post(url, headers=_ghl_headers(), json={"tags": [tag]}, timeout=20)
    if r.status_code >= 400:
        log.error("ghl_add_tag failed %s: %s", r.status_code, r.text[:300])
        return False
    return True


def ghl_move_to_stage(contact_id: str, pipeline_id: str, stage_id: str) -> bool:
    """Create an opportunity in given pipeline+stage for the contact."""
    location_id = get_ssm("GHL_LOCATION_ID", required=False) or get_ssm(
        "LOCATION_ID", required=False
    )
    url = f"{GHL_BASE}/opportunities/"
    body = {
        "pipelineId": pipeline_id,
        "pipelineStageId": stage_id,
        "locationId": location_id,
        "contactId": contact_id,
        "name": f"Cold Nurture — {contact_id}",
        "status": "open",
    }
    r = requests.post(url, headers=_ghl_headers(), json=body, timeout=20)
    if r.status_code >= 400:
        log.warning(
            "ghl_move_to_stage non-2xx %s: %s", r.status_code, r.text[:300]
        )
        return False
    return True


def ghl_send_sms(contact_id: str, message: str) -> bool:
    url = f"{GHL_BASE}/conversations/messages"
    body = {"type": "SMS", "contactId": contact_id, "message": message}
    r = requests.post(url, headers=_ghl_headers(), json=body, timeout=20)
    if r.status_code >= 400:
        log.error("ghl_send_sms failed %s: %s", r.status_code, r.text[:300])
        return False
    return True


def ghl_send_email(contact_id: str, subject: str, html: str) -> bool:
    url = f"{GHL_BASE}/conversations/messages"
    body = {
        "type": "Email",
        "contactId": contact_id,
        "subject": subject,
        "html": html,
    }
    r = requests.post(url, headers=_ghl_headers(), json=body, timeout=20)
    if r.status_code >= 400:
        log.error("ghl_send_email failed %s: %s", r.status_code, r.text[:300])
        return False
    return True


# ---------------------------------------------------------------------------
# Content selector (Touch 1..8)
# ---------------------------------------------------------------------------
def _cta_rebook_url() -> str:
    return get_ssm("GHL_CALENDAR_BOOKING_URL", required=False) or ""


def build_content(touch_number: int, contact: dict) -> dict:
    """
    Returns {sms, email_subject, email_html} personalized by touch, vertical, city.
    """
    first_name = (contact.get("firstName") or contact.get("first_name") or "there").strip()
    city = (contact.get("city") or "your city").strip()
    vertical = (contact.get("customField", {}) or {}).get("vertical") or contact.get("vertical") or "local business"
    rebook = _cta_rebook_url()

    if touch_number == 1:
        subj = f"One quick {vertical} SEO tip for {city}"
        body_text = (
            f"Hey {first_name} — sharing a quick local SEO tip for {vertical} "
            f"businesses in {city}: make sure your Google Business Profile has your "
            f"service area, hours and at least 10 photos under 6 months old. "
            f"That one change alone lifts map pack visibility ~15%."
        )
    elif touch_number == 2:
        subj = f"Who's ranking above you in {city}"
        body_text = (
            f"Hi {first_name} — quick competitor alert for {vertical} in {city}. "
            f"Three local competitors have moved into the top 3 Map Pack in the "
            f"last 30 days. Want me to run a quick gap report so you can see "
            f"what's changed?"
        )
    elif touch_number == 3:
        subj = f"Case study — {vertical} in a city like {city}"
        body_text = (
            f"{first_name}, sharing a quick win from a {vertical} client similar "
            f"to your market: went from page 2 to top 3 in the Map Pack in 74 days "
            f"by fixing 4 things. Reply 'case' and I'll send you the breakdown."
        )
    elif touch_number == 4:
        subj = "Still on your radar?"
        body_text = (
            f"Hey {first_name} — not pitching anything. Just wanted to ask: "
            f"is local SEO still something you're thinking about for your "
            f"{vertical} business in {city}? A simple yes/no works."
        )
    elif touch_number == 5:
        subj = f"SERP changes in {city} this month"
        body_text = (
            f"{first_name} — {city} {vertical} ranking data update: the top 3 "
            f"positions for your category shifted again this month. If you want "
            f"the refreshed gap report, reply 'report'."
        )
    elif touch_number == 6:
        subj = "Another quick win story"
        body_text = (
            f"Hi {first_name}, another quick case study from a {vertical} business "
            f"we worked with last quarter. Re-engagement offer attached if timing "
            f"has changed on your end."
        )
    elif touch_number == 7:
        subj = "The gap is growing"
        body_text = (
            f"{first_name} — honest read: the ranking gap between top-3 and "
            f"everyone else in {city} {vertical} is widening every quarter. "
            f"If local search is still the play, now is a better moment than "
            f"next quarter."
        )
    else:  # touch 8 — final CTA
        subj = "Final check-in"
        cta = f"\n\nBook a 15-min call: {rebook}" if rebook else ""
        body_text = (
            f"Hi {first_name} — final message in this series. If local SEO in "
            f"{city} for {vertical} is still a priority, happy to run through "
            f"your current standing on a short call. No pressure if not.{cta}"
        )

    html_body = (
        "<html><body style='font-family:Arial,sans-serif;line-height:1.5;"
        "color:#222;'><p>"
        + body_text.replace("\n\n", "</p><p>").replace("\n", "<br/>")
        + "</p></body></html>"
    )

    return {
        "sms": body_text if len(body_text) < 320 else body_text[:317] + "...",
        "email_subject": subj,
        "email_html": html_body,
    }


# ---------------------------------------------------------------------------
# RDS (best-effort — agent remains functional if RDS not reachable)
# ---------------------------------------------------------------------------
def _rds_conn():
    try:
        import psycopg2  # noqa: WPS433 (runtime-optional dep)
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
        log.warning("RDS connect failed — continuing without DB: %s", e)
        return None


def rds_check_already_enrolled(contact_id: str) -> bool:
    conn = _rds_conn()
    if not conn:
        return False
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM cold_nurture_enrollments "
                "WHERE contact_id = %s AND sequence_complete = FALSE "
                "LIMIT 1",
                (contact_id,),
            )
            return cur.fetchone() is not None
    except Exception as e:
        log.warning("rds_check_already_enrolled failed: %s", e)
        return False
    finally:
        try:
            conn.close()
        except Exception:
            pass


def rds_write_enrollment(contact_id: str, loss_reason: str) -> None:
    conn = _rds_conn()
    if not conn:
        return
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO cold_nurture_enrollments "
                "(contact_id, loss_reason) VALUES (%s, %s) "
                "ON CONFLICT (contact_id) DO UPDATE SET "
                "sequence_complete = FALSE, hand_raised = FALSE",
                (contact_id, loss_reason or ""),
            )
    except Exception as e:
        log.warning("rds_write_enrollment failed: %s", e)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def rds_write_send(contact_id: str, touch: int, sms_ok: bool, email_ok: bool) -> None:
    conn = _rds_conn()
    if not conn:
        return
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO cold_nurture_sends "
                "(contact_id, touch_number, sms_sent, email_sent) "
                "VALUES (%s, %s, %s, %s)",
                (contact_id, touch, sms_ok, email_ok),
            )
    except Exception as e:
        log.warning("rds_write_send failed: %s", e)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def rds_mark_complete(contact_id: str, reason: str) -> None:
    conn = _rds_conn()
    if not conn:
        return
    try:
        with conn, conn.cursor() as cur:
            if reason == "hand_raised":
                cur.execute(
                    "UPDATE cold_nurture_enrollments "
                    "SET sequence_complete = TRUE, hand_raised = TRUE "
                    "WHERE contact_id = %s",
                    (contact_id,),
                )
            elif reason == "converted":
                cur.execute(
                    "UPDATE cold_nurture_enrollments "
                    "SET sequence_complete = TRUE, converted = TRUE "
                    "WHERE contact_id = %s",
                    (contact_id,),
                )
            else:
                cur.execute(
                    "UPDATE cold_nurture_enrollments "
                    "SET sequence_complete = TRUE "
                    "WHERE contact_id = %s",
                    (contact_id,),
                )
    except Exception as e:
        log.warning("rds_mark_complete failed: %s", e)
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# EventBridge scheduling
# ---------------------------------------------------------------------------
def _self_lambda_arn() -> str:
    """
    ARN of the bridge Lambda (or any invoker) that will call /nurture/cold/send.
    Configured via SSM key COLD_NURTURE_TARGET_ARN. If not set, scheduling is
    skipped and cadence must be driven externally.
    """
    return get_ssm("COLD_NURTURE_TARGET_ARN", required=False)


def _rule_name(contact_id: str, touch: int) -> str:
    safe = "".join(ch for ch in contact_id if ch.isalnum() or ch in "-_")[:40]
    return f"cold-nurture-{safe}-t{touch}"


def schedule_cadence(contact_id: str, enrolled_at: datetime) -> list:
    """
    Creates 8 one-shot EventBridge schedules (at()) for each cadence day.
    Returns list of rule names created. Silent if no target ARN set.
    """
    target_arn = _self_lambda_arn()
    if not target_arn:
        log.info(
            "schedule_cadence: COLD_NURTURE_TARGET_ARN not set — "
            "skipping EventBridge scheduling"
        )
        return []

    created = []
    for touch_idx, days in enumerate(CADENCE_DAYS, start=1):
        fire_at = enrolled_at + timedelta(days=days)
        cron = (
            f"cron({fire_at.minute} {fire_at.hour} {fire_at.day} "
            f"{fire_at.month} ? {fire_at.year})"
        )
        rule = _rule_name(contact_id, touch_idx)
        try:
            _events.put_rule(
                Name=rule,
                ScheduleExpression=cron,
                State="ENABLED",
                Description=f"Cold nurture touch {touch_idx} for {contact_id}",
            )
            _events.put_targets(
                Rule=rule,
                Targets=[
                    {
                        "Id": "1",
                        "Arn": target_arn,
                        "Input": json.dumps(
                            {
                                "path": "/nurture/cold/send",
                                "contact_id": contact_id,
                                "touch_number": touch_idx,
                                "rule_name": rule,
                            }
                        ),
                    }
                ],
            )
            # If target is a Lambda, grant invoke permission (idempotent)
            if ":lambda:" in target_arn:
                try:
                    _lambda.add_permission(
                        FunctionName=target_arn.split(":")[-1],
                        StatementId=f"evb-{rule}"[:100],
                        Action="lambda:InvokeFunction",
                        Principal="events.amazonaws.com",
                        SourceArn=(
                            f"arn:aws:events:{REGION}:"
                            f"{target_arn.split(':')[4]}:rule/{rule}"
                        ),
                    )
                except ClientError as ce:
                    if ce.response["Error"]["Code"] != "ResourceConflictException":
                        log.warning(
                            "add_permission failed for %s: %s", rule, ce
                        )
            created.append(rule)
        except Exception as e:
            log.error("schedule_cadence rule %s failed: %s", rule, e)
    return created


def cancel_remaining_rules(contact_id: str) -> int:
    """Delete all EventBridge rules for this contact. Idempotent."""
    removed = 0
    for touch in range(1, len(CADENCE_DAYS) + 1):
        rule = _rule_name(contact_id, touch)
        try:
            try:
                targets = _events.list_targets_by_rule(Rule=rule).get("Targets", [])
                if targets:
                    _events.remove_targets(
                        Rule=rule, Ids=[t["Id"] for t in targets]
                    )
            except ClientError as e:
                if e.response["Error"]["Code"] != "ResourceNotFoundException":
                    raise
            _events.delete_rule(Name=rule)
            removed += 1
        except ClientError as e:
            if e.response["Error"]["Code"] != "ResourceNotFoundException":
                log.warning("cancel rule %s failed: %s", rule, e)
    return removed


# ---------------------------------------------------------------------------
# Core handlers
# ---------------------------------------------------------------------------
def handle_enroll(payload: dict) -> dict:
    contact_id = (payload.get("contact_id") or "").strip()
    if not contact_id:
        return {"ok": False, "error": "contact_id required"}

    # 1. dedupe
    if rds_check_already_enrolled(contact_id):
        log.info("enroll: %s already enrolled — skipping", contact_id)
        return {"ok": True, "status": "already_enrolled", "contact_id": contact_id}

    # 2. move to Resurrection pipeline / Cold Nurture stage
    pipeline = get_ssm("GHL_PIPELINE_RESURRECTION_ID")
    stage = get_ssm("GHL_STAGE_RESURRECTION_COLD_NURTURE")
    stage_moved = ghl_move_to_stage(contact_id, pipeline, stage)

    # 3. schedule 8 cadence rules
    enrolled_at = datetime.now(timezone.utc)
    rules = schedule_cadence(contact_id, enrolled_at)

    # 4. write enrollment record
    rds_write_enrollment(contact_id, payload.get("loss_reason") or "")

    log.info(
        "enroll ok contact=%s stage_moved=%s rules=%d",
        contact_id,
        stage_moved,
        len(rules),
    )
    return {
        "ok": True,
        "status": "enrolled",
        "contact_id": contact_id,
        "stage_moved": stage_moved,
        "rules_scheduled": len(rules),
        "enrolled_at": enrolled_at.isoformat(),
    }


def handle_send(payload: dict) -> dict:
    contact_id = (payload.get("contact_id") or "").strip()
    touch_number = int(payload.get("touch_number") or 0)
    rule_name = payload.get("rule_name") or ""

    if not contact_id or touch_number < 1 or touch_number > len(CADENCE_DAYS):
        return {"ok": False, "error": "contact_id and valid touch_number required"}

    # 1. re-query GHL contact
    contact = ghl_get_contact(contact_id)
    if not contact:
        log.warning("send: contact %s not found — aborting", contact_id)
        if rule_name:
            try:
                _events.delete_rule(Name=rule_name)
            except Exception:
                pass
        return {"ok": False, "error": "contact_not_found", "contact_id": contact_id}

    # 2. hand-raise / conversion guard
    if ghl_contact_has_tag(contact, "MOD:HAND_RAISED"):
        log.info("send: %s hand-raised — stopping sequence", contact_id)
        cancel_remaining_rules(contact_id)
        rds_mark_complete(contact_id, "hand_raised")
        return {"ok": True, "status": "stopped_hand_raised", "contact_id": contact_id}

    if ghl_contact_has_tag(contact, "REL:CLIENT"):
        log.info("send: %s converted — stopping sequence", contact_id)
        cancel_remaining_rules(contact_id)
        rds_mark_complete(contact_id, "converted")
        return {"ok": True, "status": "stopped_converted", "contact_id": contact_id}

    # 3. build content
    content = build_content(touch_number, contact)

    # 4. send SMS + email
    sms_ok = ghl_send_sms(contact_id, content["sms"]) if contact.get("phone") else False
    email_ok = (
        ghl_send_email(contact_id, content["email_subject"], content["email_html"])
        if contact.get("email")
        else False
    )

    # 5. log send
    rds_write_send(contact_id, touch_number, sms_ok, email_ok)

    # 6. last touch → mark complete
    if touch_number == len(CADENCE_DAYS):
        rds_mark_complete(contact_id, "completed")

    log.info(
        "send ok contact=%s touch=%d sms=%s email=%s",
        contact_id,
        touch_number,
        sms_ok,
        email_ok,
    )
    return {
        "ok": True,
        "status": "sent",
        "contact_id": contact_id,
        "touch_number": touch_number,
        "sms_sent": sms_ok,
        "email_sent": email_ok,
    }


# ---------------------------------------------------------------------------
# AgentCore entrypoint
# ---------------------------------------------------------------------------
@app.entrypoint
def invoke(payload: dict) -> dict:
    """
    Payload shape:
      {"path": "/nurture/cold/enroll" | "/nurture/cold/send", ...fields...}

    If 'path' is omitted, defaults to enroll (webhook mode).
    """
    try:
        path = (payload.get("path") or "/nurture/cold/enroll").rstrip("/")
        t0 = time.time()

        if path == "/nurture/cold/enroll":
            out = handle_enroll(payload)
        elif path == "/nurture/cold/send":
            out = handle_send(payload)
        elif path in ("/ping", "/health"):
            out = {"ok": True, "agent": "coldNurture", "status": "healthy"}
        else:
            out = {"ok": False, "error": f"unknown path: {path}"}

        out["elapsed_ms"] = int((time.time() - t0) * 1000)
        return out

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
