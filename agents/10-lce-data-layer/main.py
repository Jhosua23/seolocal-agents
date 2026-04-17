"""
================================================================
Agent 10 — LCE Data Layer
================================================================
AgentCore name  : lceDataLayer
Phase           : 1 - Lead Gen & Management
Sequence        : 10 of 15
Priority        : P1
Runtime         : Python 3.12 / ARM64
Version         : 1.0.0

NO RDS — all data read from GHL contact custom fields.
Cache   : enrichment_date GHL field used as freshness check.
================================================================

SSM Parameters:
  GHL_API_KEY              - SecureString  (already in SSM)
  GOOGLE_PLACES_API_KEY    - SecureString  (already in SSM)
  PAGESPEED_API_KEY        - SecureString  (already in SSM)
  DATAFORSEO_LOGIN         - SecureString  (already in SSM)
  DATAFORSEO_PASSWORD      - SecureString  (already in SSM)

  Revenue estimates (ask Chuck to add):
  REVENUE_ESTIMATES_HVAC     - String  e.g. "$3,000-$8,000/month"
  REVENUE_ESTIMATES_DENTAL   - String  e.g. "$5,000-$15,000/month"
  REVENUE_ESTIMATES_ROOFING  - String  e.g. "$4,000-$10,000/month"
  REVENUE_ESTIMATES_LEGAL    - String  e.g. "$8,000-$20,000/month"
  REVENUE_ESTIMATES_MEDSPA   - String  e.g. "$3,000-$9,000/month"
  REVENUE_ESTIMATES_DEFAULT  - String  e.g. "$2,000-$6,000/month"

Trigger  : GET /lce/data/{contact_id}
           payload: {"contact_id": "...", "force_refresh": false}
================================================================
"""

import asyncio
import hashlib
import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
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
        "agent":   "lceDataLayer",
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
GHL_BASE      = "https://services.leadconnectorhq.com"
PLACES_BASE   = "https://maps.googleapis.com/maps/api/place"
PAGESPEED_BASE = "https://www.googleapis.com/pagespeedonline/v5/runPagespeed"
DATAFORSEO_BASE = "https://api.dataforseo.com/v3"
REGION        = "us-east-1"
CODE_VERSION  = "1.0.0"

CACHE_FRESHNESS_HOURS = 4    # re-fetch if enrichment older than this
SPEED_BENCHMARK       = 65   # industry benchmark mobile score
PARALLEL_TIMEOUT      = 12   # seconds for parallel API calls


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
                method, url, headers=headers, timeout=10, **kwargs
            )
        except requests.RequestException as exc:
            if attempt == retries:
                raise
            time.sleep(2 ** attempt)
            continue

        if resp.status_code == 401:
            raise RuntimeError("GHL auth failed")
        if resp.status_code == 429:
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
# GHL — Read contact
# ----------------------------------------------------------------
def ghl_get_contact(contact_id: str) -> dict:
    resp = _ghl("GET", f"{GHL_BASE}/contacts/{contact_id}")
    if resp.status_code == 200:
        return resp.json().get("contact", {})
    log("warning", "ghl_contact_not_found",
        contact_id_hash=_h(contact_id), status=resp.status_code)
    return {}


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


def _hours_since(dt_str: str) -> float:
    if not dt_str:
        return 9999.0
    try:
        dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        return (datetime.now(timezone.utc) - dt).total_seconds() / 3600
    except Exception:
        return 9999.0


def _gbp_completeness_grade(score: int) -> str:
    if score >= 90:
        return "A"
    if score >= 70:
        return "B"
    if score >= 50:
        return "C"
    return "D"


def _get_revenue_estimate(vertical: str) -> str:
    key = f"REVENUE_ESTIMATES_{vertical.upper().replace(' ', '_')}"
    val = get_ssm(key, "")
    if not val:
        val = get_ssm("REVENUE_ESTIMATES_DEFAULT", "$2,000–$6,000/month")
    return val


# ----------------------------------------------------------------
# DataForSEO — Map Pack query (REST, not gRPC)
# ----------------------------------------------------------------
def fetch_map_pack(keyword: str, city: str) -> dict:
    """
    Returns top 3 Map Pack results for keyword in city.
    Uses DataForSEO SERP API — REST via requests (gRPC blocked in AgentCore).
    """
    try:
        login    = get_ssm("DATAFORSEO_LOGIN")
        password = get_ssm("DATAFORSEO_PASSWORD")

        payload = [{
            "keyword":       f"{keyword} {city}",
            "location_name": f"{city},United States",
            "language_name": "English",
            "depth":         10,
        }]
        resp = requests.post(
            f"{DATAFORSEO_BASE}/serp/google/maps/live/advanced",
            auth=(login, password),
            json=payload,
            timeout=PARALLEL_TIMEOUT,
        )
        data = resp.json()
        tasks = data.get("tasks", [])
        if not tasks:
            return {}

        items = tasks[0].get("result", [{}])[0].get("items", [])
        competitors = []
        for item in items[:3]:
            competitors.append({
                "name":     item.get("title", ""),
                "position": item.get("rank_group", 0),
                "reviews":  item.get("reviews_count", 0),
                "rating":   item.get("rating", {}).get("value", 0),
            })
        return {"competitors": competitors}

    except Exception as exc:
        log("warning", "dataforseo_map_pack_failed", error=str(exc))
        return {}


# ----------------------------------------------------------------
# Google Places — GBP data (REST, not gRPC)
# ----------------------------------------------------------------
def fetch_gbp_data(business_name: str, city: str) -> dict:
    try:
        api_key = get_ssm("GOOGLE_PLACES_API_KEY")
        resp = requests.get(
            f"{PLACES_BASE}/findplacefromtext/json",
            params={
                "input":     f"{business_name} {city}",
                "inputtype": "textquery",
                "fields":    "rating,user_ratings_total,business_status",
                "key":       api_key,
            },
            timeout=PARALLEL_TIMEOUT,
        )
        candidates = resp.json().get("candidates", [])
        if not candidates:
            return {}
        c = candidates[0]
        return {
            "rating":       c.get("rating", 0),
            "review_count": c.get("user_ratings_total", 0),
            "status":       c.get("business_status", "UNKNOWN"),
        }
    except Exception as exc:
        log("warning", "places_gbp_failed", error=str(exc))
        return {}


# ----------------------------------------------------------------
# PageSpeed — mobile score (REST, not gRPC)
# ----------------------------------------------------------------
def fetch_pagespeed(website_url: str) -> dict:
    try:
        api_key = get_ssm("PAGESPEED_API_KEY")
        resp = requests.get(
            PAGESPEED_BASE,
            params={
                "url":      website_url,
                "strategy": "mobile",
                "key":      api_key,
            },
            timeout=PARALLEL_TIMEOUT,
        )
        score = int(
            resp.json()
                .get("lighthouseResult", {})
                .get("categories", {})
                .get("performance", {})
                .get("score", 0) * 100
        )
        return {"mobile_score": score}
    except Exception as exc:
        log("warning", "pagespeed_failed", error=str(exc))
        return {}


# ----------------------------------------------------------------
# Parallel live fetch (cache miss path)
# ----------------------------------------------------------------
def fetch_live_data(business_name: str, city: str,
                    vertical: str, website_url: str) -> dict:
    """
    Runs DataForSEO + Places + PageSpeed in parallel threads.
    Target: < 12 seconds total.
    """
    result = {}

    def run_map_pack():
        return "map_pack", fetch_map_pack(vertical or business_name, city)

    def run_gbp():
        return "gbp", fetch_gbp_data(business_name, city)

    def run_speed():
        return ("speed", fetch_pagespeed(website_url)) if website_url else ("speed", {})

    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [
            executor.submit(run_map_pack),
            executor.submit(run_gbp),
            executor.submit(run_speed),
        ]
        for future in as_completed(futures, timeout=PARALLEL_TIMEOUT + 2):
            try:
                key, val = future.result()
                result[key] = val
            except Exception as exc:
                log("warning", "parallel_fetch_error", error=str(exc))

    return result


# ----------------------------------------------------------------
# Build LCE response from GHL contact + live/cached data
# ----------------------------------------------------------------
def build_response(contact: dict, live: dict,
                   contact_id: str, data_age_minutes: float) -> dict:
    # Extract from GHL contact
    business_name   = contact.get("companyName") or contact.get("firstName", "")
    city            = _get_field(contact, "city") or ""
    vertical        = _get_field(contact, "vertical") or ""
    audit_score     = _get_field(contact, "audit_score")
    audit_grade     = _get_field(contact, "audit_grade")
    issue_1         = _get_field(contact, "audit_top_issue_1")
    issue_2         = _get_field(contact, "audit_top_issue_2")
    issue_3         = _get_field(contact, "audit_top_issue_3")
    ai_vis_score    = _get_field(contact, "ai_visibility_score")
    gbp_review_count = int(_get_field(contact, "gbp_review_count") or 0)
    gbp_rating      = float(_get_field(contact, "gbp_rating") or 0)
    gbp_completeness = int(_get_field(contact, "gbp_completeness_score") or 60)
    website_url     = _get_field(contact, "your_website") or contact.get("website", "")

    audit_issues = [i for i in [issue_1, issue_2, issue_3] if i]

    # Map Pack data
    map_pack_data = live.get("map_pack", {})
    competitors   = map_pack_data.get("competitors", [])
    comp_1        = competitors[0] if competitors else {}
    comp_2        = competitors[1] if len(competitors) > 1 else {}
    map_pack_gap  = (comp_1.get("reviews", 0) - gbp_review_count) if comp_1 else 0

    # GBP live data (supplement if available)
    gbp_live = live.get("gbp", {})
    if gbp_live.get("review_count"):
        gbp_review_count = gbp_live["review_count"]
    if gbp_live.get("rating"):
        gbp_rating = gbp_live["rating"]

    completeness_grade = _gbp_completeness_grade(gbp_completeness)

    # Speed data
    speed_data    = live.get("speed", {})
    mobile_score  = speed_data.get("mobile_score", 0)
    speed_delta   = mobile_score - SPEED_BENCHMARK if mobile_score else None

    # AI visibility
    ai_status = "Unknown"
    if ai_vis_score:
        score_int = int(ai_vis_score) if str(ai_vis_score).isdigit() else 0
        if score_int >= 66:
            ai_status = "Appearing"
        elif score_int > 0:
            ai_status = "Partial"
        else:
            ai_status = "Not appearing"

    # Revenue estimate
    revenue_estimate = _get_revenue_estimate(vertical)

    return {
        "contact_id":          contact_id,
        "business_name":       business_name,
        "city":                city,
        "vertical":            vertical,
        "audit_score":         int(audit_score) if audit_score else None,
        "audit_grade":         audit_grade,
        "audit_top_issues":    audit_issues,
        "map_pack": {
            "prospect_ranking":      False if not comp_1 else (map_pack_gap <= 0),
            "competitor_1":          comp_1,
            "competitor_2":          comp_2,
            "map_pack_gap":          max(map_pack_gap, 0),
            "estimated_monthly_loss": revenue_estimate,
        },
        "gbp": {
            "rating":              gbp_rating,
            "review_count":        gbp_review_count,
            "completeness_score":  gbp_completeness,
            "completeness_grade":  completeness_grade,
        },
        "website": {
            "url":               website_url,
            "mobile_speed_score": mobile_score,
            "speed_delta":        speed_delta,
            "speed_benchmark":    SPEED_BENCHMARK,
        },
        "ai_visibility": {
            "status": ai_status,
            "score":  int(ai_vis_score) if ai_vis_score and str(ai_vis_score).isdigit() else 0,
        },
        "intel_generated_at": datetime.now(timezone.utc).isoformat(),
        "data_age_minutes":   round(data_age_minutes, 1),
        "agent":              "lceDataLayer",
        "version":            CODE_VERSION,
    }


# ----------------------------------------------------------------
# CORE AGENT LOGIC
# ----------------------------------------------------------------
def run_lce_data_layer(payload: dict) -> dict:
    start_ms = time.time()

    contact_id    = (payload.get("contact_id") or "").strip()
    force_refresh = payload.get("force_refresh", False)

    if not contact_id:
        return {"error": "missing_contact_id"}

    log("info", "lce_start",
        contact_id_hash=_h(contact_id), force_refresh=force_refresh)

    # --- Step 1: Load GHL contact ---
    contact = ghl_get_contact(contact_id)
    if not contact:
        return {"error": "contact_not_found", "contact_id": contact_id}

    business_name = contact.get("companyName") or contact.get("firstName", "")
    city          = _get_field(contact, "city")
    vertical      = _get_field(contact, "vertical")
    website_url   = _get_field(contact, "your_website") or contact.get("website", "")

    # --- Step 2: Check data freshness via GHL enrichment_date field ---
    enrichment_date = _get_field(contact, "enrichment_date")
    data_age_hours  = _hours_since(enrichment_date)
    data_age_mins   = data_age_hours * 60
    cache_fresh     = (data_age_hours < CACHE_FRESHNESS_HOURS) and not force_refresh

    live_data = {}

    if cache_fresh:
        # Cache hit — serve from GHL custom fields
        log("info", "cache_hit",
            contact_id_hash=_h(contact_id),
            data_age_hours=round(data_age_hours, 2))
    else:
        # Cache miss — fetch live data in parallel
        log("info", "cache_miss_fetching_live",
            contact_id_hash=_h(contact_id),
            force_refresh=force_refresh,
            data_age_hours=round(data_age_hours, 2))

        live_data = fetch_live_data(business_name, city, vertical, website_url)

    # --- Steps 4 & 5: Build response ---
    response = build_response(contact, live_data, contact_id, data_age_mins)

    response_ms = round((time.time() - start_ms) * 1000)

    # --- Step 7: CloudWatch log ---
    log("info", "lce_complete",
        contact_id_hash=_h(contact_id),
        cache_hit=cache_fresh,
        data_age_minutes=round(data_age_mins, 1),
        response_time_ms=response_ms,
        version=CODE_VERSION)

    return response


# ----------------------------------------------------------------
# AgentCore entrypoint
# ----------------------------------------------------------------
if _SDK:
    app = BedrockAgentCoreApp()

    @app.entrypoint
    def lce_data_layer(payload: dict) -> dict:
        try:
            return run_lce_data_layer(payload)
        except SystemExit:
            raise
        except BaseException as exc:
            log("error", "unhandled_exception",
                error=str(exc), exc_type=type(exc).__name__)
            return {
                "error":   "INTERNAL_ERROR",
                "message": str(exc),
            }

   
    app.run()
