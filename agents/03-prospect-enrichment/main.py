"""
================================================================
Agent 03 — Prospect Enrichment
================================================================
AgentCore name  : prospectEnrichment
Phase           : 1 - Lead Generation
Sequence        : 3 of 15
Priority        : P1
Runtime         : Python 3.12 / ARM64
Version         : 1.0.0

NO RDS — GHL is the single source of truth.
Dedup   : tag  enrichment-complete  on contact
State   : all enrichment data stored as GHL custom fields
================================================================

SSM Parameters required (both already in your AWS SSM):
  GOOGLE_PLACES_API_KEY  - SecureString
  GHL_API_KEY            - SecureString
  GHL_DEFAULT_ASSIGNEE_ID - String (4yPiyMcdXbdTUh0Om8m2jp)

Trigger  : GHL Webhook POST to /enrich/prospect
           Fires when new contact created with audit-completed tag

Processing:
  1. Extract domain from email
  2. Google Places findplacefromtext (3 attempts, broadening search)
  3. Cross-reference phone if provided
  4. HTTP HEAD check on website
  5. Social profile presence check (Facebook, Instagram)
  6. Write all data to GHL custom fields
  7. Apply GHL tags
  8. Log to CloudWatch
================================================================
"""

import hashlib
import json
import logging
import re
import time
from datetime import datetime, timezone
from urllib.parse import quote_plus

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
        "agent":   "prospectEnrichment",
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
GHL_BASE    = "https://services.leadconnectorhq.com"
LOCATION_ID = "uXRl9WpDjS7LFjeYfQqD"
REGION      = "us-east-1"

PLACES_BASE = "https://maps.googleapis.com/maps/api/place"
PLACES_FIELDS = (
    "name,rating,user_ratings_total,formatted_address,"
    "formatted_phone_number,website,place_id,types,business_status"
)

# GHL tags used by this agent
TAG_ENRICHED       = "enrichment-complete"
TAG_GBP_FOUND      = "gbp-found"
TAG_GBP_NOT_FOUND  = "gbp-not-found"
TAG_GBP_FAILED     = "gbp-lookup-failed"
TAG_LOW_REVIEWS    = "low-reviews"
TAG_NO_WEBSITE     = "no-website"

LOW_REVIEW_THRESHOLD = 10
HTTP_TIMEOUT         = 8   # seconds for website/social HEAD checks


# ----------------------------------------------------------------
# SSM — cached, NextToken pagination mandatory per dev guide
# ----------------------------------------------------------------
_ssm_cache: dict = {}


def _ssm_client():
    return boto3.client("ssm", region_name=REGION)


def get_ssm(name: str) -> str:
    if name in _ssm_cache:
        return _ssm_cache[name]
    val = _ssm_client().get_parameter(
        Name=name, WithDecryption=True
    )["Parameter"]["Value"]
    _ssm_cache[name] = val
    return val


# ----------------------------------------------------------------
# GHL HTTP helper — rate-limit + retry
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
                status=resp.status_code, attempt=attempt)
            if attempt == retries:
                raise RuntimeError(f"GHL 5xx: {resp.status_code}")
            time.sleep(2 ** attempt)
            continue

        return resp

    raise RuntimeError(f"GHL request failed after {retries + 1} attempts")


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
            contact_id_hash=_h(contact_id), status=resp.status_code,
            body=resp.text[:300])
    return ok


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
            contact_id_hash=_h(contact_id), tags=tags,
            status=resp.status_code)
    return ok


# ----------------------------------------------------------------
# GHL — Internal note
# ----------------------------------------------------------------
def ghl_add_note(contact_id: str, body: str) -> bool:
    resp = _ghl(
        "POST",
        f"{GHL_BASE}/contacts/{contact_id}/notes",
        json={"body": body, "type": "Note"},
    )
    ok = resp.status_code in (200, 201)
    if not ok:
        log("warning", "ghl_add_note_failed",
            contact_id_hash=_h(contact_id), status=resp.status_code)
    return ok


# ----------------------------------------------------------------
# GHL — Urgent task for Jay Leonard
# ----------------------------------------------------------------
def ghl_create_task(contact_id: str, title: str, body: str) -> bool:
    from datetime import timedelta
    resp = _ghl(
        "POST",
        f"{GHL_BASE}/contacts/{contact_id}/tasks",
        json={
            "title":      title,
            "body":       body,
            "assignedTo": get_ssm("GHL_DEFAULT_ASSIGNEE_ID"),
            "contactId":  contact_id,
            "dueDate":    (datetime.now(timezone.utc) + timedelta(hours=4)).isoformat() + "Z",
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
    """Hash contact_id — never log PII to CloudWatch."""
    return hashlib.sha256(contact_id.encode()).hexdigest()[:16]


def _slugify(text: str) -> str:
    """Convert business name to social URL slug."""
    text = text.lower().strip()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[\s_-]+", "", text)
    return text


def _extract_domain(email: str) -> str:
    """owner@phoenixhvacpro.com → phoenixhvacpro.com"""
    if "@" in email:
        return email.split("@")[1].strip().lower()
    return ""


def _normalize_phone(phone: str) -> str:
    """Strip all non-digits for comparison."""
    return re.sub(r"\D", "", phone or "")


# ----------------------------------------------------------------
# Google Places API — findplacefromtext
# Tries up to 3 queries with broadening search
# ----------------------------------------------------------------
def places_find(query: str) -> dict | None:
    """
    Call Places findplacefromtext.
    Returns candidate dict or None.
    Uses REST via requests — NOT gRPC (blocked in AgentCore).
    """
    api_key = get_ssm("GOOGLE_PLACES_API_KEY")
    url = f"{PLACES_BASE}/findplacefromtext/json"
    params = {
        "input":     query,
        "inputtype": "textquery",
        "fields":    PLACES_FIELDS,
        "key":       api_key,
    }
    try:
        resp = requests.get(url, params=params, timeout=10)
        data = resp.json()
        candidates = data.get("candidates", [])
        if candidates:
            return candidates[0]
    except Exception as exc:
        log("warning", "places_api_exception", query=query, error=str(exc))
    return None


def places_search(
    business_name: str,
    city: str,
    state: str,
    domain: str,
) -> dict | None:
    """
    Three-attempt broadening search per spec:
      Attempt 1: "{business_name} {city} {state}"
      Attempt 2: "{business_name}" only
      Attempt 3: "{domain} {city}"
    Returns first non-None result or None.
    """
    queries = [
        f"{business_name} {city} {state}".strip(),
        business_name.strip(),
        f"{domain} {city}".strip() if domain else None,
    ]
    for q in queries:
        if not q:
            continue
        log("info", "places_search_attempt", query=q)
        result = places_find(q)
        if result:
            log("info", "places_search_hit", query=q,
                place_id=result.get("place_id", ""))
            return result
        time.sleep(0.3)   # brief pause between attempts
    return None


# ----------------------------------------------------------------
# Website reachability check
# ----------------------------------------------------------------
def check_website(url: str) -> tuple:
    """
    Returns (reachable: bool, is_https: bool, final_url: str).
    Uses HEAD with redirect follow — fast, no body downloaded.
    """
    if not url:
        return False, False, ""
    if not url.startswith("http"):
        url = "https://" + url
    try:
        resp = requests.head(
            url,
            timeout=HTTP_TIMEOUT,
            allow_redirects=True,
            headers={"User-Agent": "Mozilla/5.0"},
        )
        final_url = resp.url
        reachable = resp.status_code < 400
        is_https  = final_url.startswith("https://")
        return reachable, is_https, final_url
    except Exception:
        # Try HTTP fallback if HTTPS fails
        try:
            http_url = url.replace("https://", "http://")
            resp = requests.head(
                http_url,
                timeout=HTTP_TIMEOUT,
                allow_redirects=True,
                headers={"User-Agent": "Mozilla/5.0"},
            )
            return resp.status_code < 400, False, resp.url
        except Exception:
            return False, False, url


# ----------------------------------------------------------------
# Social profile presence check — HEAD only, no scraping
# ----------------------------------------------------------------
def check_social(slug: str) -> tuple:
    """
    Returns (facebook_url | None, instagram_url | None).
    Basic HTTP HEAD check — presence only.
    """
    fb_url  = None
    ig_url  = None

    if not slug:
        return fb_url, ig_url

    fb_candidate = f"https://www.facebook.com/{slug}"
    ig_candidate = f"https://www.instagram.com/{slug}"

    for url, attr in [(fb_candidate, "fb"), (ig_candidate, "ig")]:
        try:
            resp = requests.head(
                url,
                timeout=HTTP_TIMEOUT,
                allow_redirects=True,
                headers={"User-Agent": "Mozilla/5.0"},
            )
            if resp.status_code == 200:
                if attr == "fb":
                    fb_url = url
                else:
                    ig_url = url
        except Exception:
            pass

    return fb_url, ig_url


# ----------------------------------------------------------------
# Phone cross-reference validator
# ----------------------------------------------------------------
def phones_match(payload_phone: str, places_phone: str) -> bool:
    """
    Compare last 10 digits of both numbers.
    Returns True if match or either is missing.
    """
    p1 = _normalize_phone(payload_phone)[-10:]
    p2 = _normalize_phone(places_phone)[-10:]
    if not p1 or not p2:
        return True   # can't verify — treat as match
    return p1 == p2


# ----------------------------------------------------------------
# Build GHL custom fields list from enrichment data
# ----------------------------------------------------------------
def build_ghl_fields(enrichment: dict) -> list:
    """
    Map enrichment dict to GHL customFields format.
    All values cast to string — GHL expects string field_value.
    """
    mapping = {
        "gbp_rating":       str(enrichment.get("gbp_rating", "")),
        "gbp_review_count": str(enrichment.get("gbp_review_count", "")),
        "gbp_place_id":     enrichment.get("gbp_place_id", ""),
        "gbp_category":     enrichment.get("gbp_category", ""),
        "gbp_address":      enrichment.get("gbp_address", ""),
        "gbp_website":      enrichment.get("gbp_website", ""),
        "gbp_phone":        enrichment.get("gbp_phone", ""),
        "gbp_status":       enrichment.get("gbp_status", "NOT_FOUND"),
        "website_reachable": "true" if enrichment.get("website_reachable") else "false",
        "website_https":     "true" if enrichment.get("website_https") else "false",
        "enrichment_date":   enrichment.get("enrichment_date", ""),
        "facebook_url":      enrichment.get("facebook_url", ""),
        "instagram_url":     enrichment.get("instagram_url", ""),
    }
    return [
        {"key": k, "field_value": v}
        for k, v in mapping.items()
        if v  # only write non-empty values
    ]


# ----------------------------------------------------------------
# Determine tags to apply from enrichment data
# ----------------------------------------------------------------
def build_tags(enrichment: dict, gbp_found: bool) -> list:
    tags = []

    if gbp_found:
        tags.append(TAG_GBP_FOUND)
        review_count = enrichment.get("gbp_review_count", 0) or 0
        if review_count < LOW_REVIEW_THRESHOLD:
            tags.append(TAG_LOW_REVIEWS)
    else:
        tags.append(TAG_GBP_NOT_FOUND)

    if not enrichment.get("website_reachable"):
        tags.append(TAG_NO_WEBSITE)

    tags.append(TAG_ENRICHED)
    return tags


# ----------------------------------------------------------------
# CORE AGENT LOGIC
# ----------------------------------------------------------------
def run_enrichment(payload: dict) -> dict:
    now = datetime.now(timezone.utc)

    # --- Validate required fields ---
    contact_id    = (payload.get("contact_id") or "").strip()
    email         = (payload.get("email") or "").strip()
    business_name = (payload.get("business_name") or "").strip()
    city          = (payload.get("city") or "").strip()

    if not contact_id:
        return {"status": "error", "code": "MISSING_CONTACT_ID"}
    if not email:
        return {"status": "error", "code": "MISSING_EMAIL"}
    if not business_name:
        return {"status": "error", "code": "MISSING_BUSINESS_NAME"}

    state = (payload.get("state") or "").strip()
    phone = (payload.get("phone") or "").strip()

    log("info", "enrichment_start",
        contact_id_hash=_h(contact_id),
        business_name=business_name,
        city=city)

    # --- Step 1: Dedup check via GHL tag ---
    contact = ghl_get_contact(contact_id)
    if not contact:
        log("error", "contact_not_found",
            contact_id_hash=_h(contact_id))
        return {"status": "error", "code": "CONTACT_NOT_FOUND"}

    if TAG_ENRICHED in contact.get("tags", []):
        log("info", "already_enriched_skip",
            contact_id_hash=_h(contact_id))
        return {"status": "skipped", "reason": "already_enriched"}

    # --- Step 1: Extract domain from email ---
    domain = _extract_domain(email)
    log("info", "domain_extracted", domain=domain)

    # --- Step 2 & 3: Google Places search with broadening ---
    enrichment: dict = {
        "gbp_status":       "NOT_FOUND",
        "enrichment_date":  now.isoformat(),
    }
    gbp_found  = False
    place_data = None

    try:
        place_data = places_search(business_name, city, state, domain)
    except Exception as exc:
        log("error", "places_search_exception", error=str(exc))
        # Non-fatal — continue with empty enrichment

    if place_data:
        gbp_found = True

        # --- Step 4: Extract GBP data ---
        enrichment["gbp_rating"]       = place_data.get("rating")
        enrichment["gbp_review_count"]  = place_data.get("user_ratings_total")
        enrichment["gbp_place_id"]      = place_data.get("place_id", "")
        enrichment["gbp_address"]       = place_data.get("formatted_address", "")
        enrichment["gbp_phone"]         = place_data.get("formatted_phone_number", "")
        enrichment["gbp_website"]       = place_data.get("website", "")
        enrichment["gbp_status"]        = place_data.get("business_status", "OPERATIONAL")

        # Extract primary category from types list
        types = place_data.get("types", [])
        enrichment["gbp_category"] = types[0].replace("_", " ").title() if types else ""

        log("info", "gbp_found",
            contact_id_hash=_h(contact_id),
            place_id=enrichment["gbp_place_id"],
            rating=enrichment["gbp_rating"],
            reviews=enrichment["gbp_review_count"],
            status=enrichment["gbp_status"])

        # --- Step 5: Cross-reference phone ---
        if phone and enrichment["gbp_phone"]:
            if not phones_match(phone, enrichment["gbp_phone"]):
                log("warning", "phone_mismatch",
                    contact_id_hash=_h(contact_id),
                    payload_phone_last4=_normalize_phone(phone)[-4:],
                    places_phone_last4=_normalize_phone(enrichment["gbp_phone"])[-4:])
                # Log only — do not block enrichment

        # --- Steps 6 & 7: Website reachability ---
        website_url = enrichment.get("gbp_website", "")
        if not website_url:
            # Fall back to domain from email
            website_url = f"https://{domain}" if domain else ""

        if website_url:
            reachable, is_https, final_url = check_website(website_url)
            enrichment["website_reachable"] = reachable
            enrichment["website_https"]     = is_https
            enrichment["gbp_website"]       = final_url or website_url
            log("info", "website_checked",
                contact_id_hash=_h(contact_id),
                reachable=reachable, https=is_https)
        else:
            enrichment["website_reachable"] = False
            enrichment["website_https"]     = False

    else:
        # GBP not found
        log("warning", "gbp_not_found",
            contact_id_hash=_h(contact_id),
            business_name=business_name, city=city)

        # Still check website from email domain
        if domain:
            website_url = f"https://{domain}"
            reachable, is_https, final_url = check_website(website_url)
            enrichment["website_reachable"] = reachable
            enrichment["website_https"]     = is_https
            enrichment["gbp_website"]       = final_url if reachable else ""
            log("info", "website_checked_from_domain",
                contact_id_hash=_h(contact_id),
                reachable=reachable, https=is_https)
        else:
            enrichment["website_reachable"] = False
            enrichment["website_https"]     = False

    # --- Step 8: Social profile check ---
    slug = _slugify(business_name)
    fb_url, ig_url = check_social(slug)
    enrichment["facebook_url"]  = fb_url or ""
    enrichment["instagram_url"] = ig_url or ""
    log("info", "social_checked",
        contact_id_hash=_h(contact_id),
        has_facebook=bool(fb_url),
        has_instagram=bool(ig_url))

    # --- Step 9: Compile and write to GHL ---
    fields = build_ghl_fields(enrichment)
    tags   = build_tags(enrichment, gbp_found)

    fields_ok = ghl_update_fields(contact_id, fields)
    if not fields_ok:
        # Log full enrichment data to CloudWatch so nothing is lost
        log("error", "ghl_fields_update_failed_data_logged",
            contact_id_hash=_h(contact_id),
            enrichment_data=json.dumps(enrichment))

    # --- Step 10: Apply tags ---
    tags_ok = ghl_add_tags(contact_id, tags)

    # --- Step 11: Add GHL note if GBP not found ---
    if not gbp_found:
        ghl_add_note(
            contact_id,
            f"ENRICHMENT: Google Business Profile not found for '{business_name}' "
            f"in {city}. Manual GBP lookup may be needed before the sales call."
        )

    # --- Step 12: CloudWatch execution log ---
    log("info", "enrichment_complete",
        contact_id_hash=_h(contact_id),
        gbp_found=gbp_found,
        website_reachable=enrichment.get("website_reachable", False),
        has_facebook=bool(fb_url),
        has_instagram=bool(ig_url),
        review_count=enrichment.get("gbp_review_count"),
        rating=enrichment.get("gbp_rating"),
        fields_written=len(fields),
        tags_applied=tags,
        fields_ok=fields_ok,
        tags_ok=tags_ok,
        version="1.0.0")

    return {
        "status":            "enriched",
        "contact_id":        contact_id,
        "gbp_found":         gbp_found,
        "gbp_status":        enrichment.get("gbp_status", "NOT_FOUND"),
        "gbp_rating":        enrichment.get("gbp_rating"),
        "gbp_review_count":  enrichment.get("gbp_review_count"),
        "website_reachable": enrichment.get("website_reachable", False),
        "has_facebook":      bool(fb_url),
        "has_instagram":     bool(ig_url),
        "tags_applied":      tags,
        "fields_written":    len(fields),
        "agent":             "prospectEnrichment",
        "version":           "1.0.0",
    }


# ----------------------------------------------------------------
# AgentCore entrypoint
# Rules (from developer guide):
#   1. payload: dict only — no context parameter
#   2. app.run() is mandatory
#   3. Catch SystemExit and BaseException separately
# ----------------------------------------------------------------
if _SDK:
    app = BedrockAgentCoreApp()

    @app.entrypoint
    def prospect_enrichment(payload: dict) -> dict:
        try:
            return run_enrichment(payload)
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
