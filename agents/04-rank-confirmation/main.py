"""
================================================================
Agent 04 — Rank Confirmation Agent
================================================================
AgentCore name  : rankConfirmation
Phase           : 1 - Lead Generation
Sequence        : 4 of 15
Priority        : P1
Complexity      : Low
Runtime         : Python 3.12 / ARM64
Version         : 1.0.0

SSM Parameters required:
  DATAFORSEO_LOGIN     - SecureString
  DATAFORSEO_PASSWORD  - SecureString

Trigger  : POST /rank/check from website Check My Ranking widget
================================================================
"""

import base64
import json
import logging
from datetime import datetime, timezone

import boto3
import requests

# ----------------------------------------------------------------
# Logging
# ----------------------------------------------------------------
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def log(level: str, event: str, **kwargs) -> None:
    record = {
        "level":  level,
        "event":  event,
        "ts":     datetime.now(timezone.utc).isoformat(),
        "agent":  "rankConfirmation",
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
REGION          = "us-east-1"
CODE_VERSION    = "1.0.0"
DATAFORSEO_BASE = "https://api.dataforseo.com/v3"


# ----------------------------------------------------------------
# SSM cache
# ----------------------------------------------------------------
_ssm_cache: dict = {}


def get_ssm(name: str) -> str:
    if name in _ssm_cache:
        return _ssm_cache[name]
    val = boto3.client("ssm", region_name=REGION).get_parameter(
        Name=name, WithDecryption=True
    )["Parameter"]["Value"]
    _ssm_cache[name] = val
    return val


# ----------------------------------------------------------------
# DataForSEO helper
# ----------------------------------------------------------------
def get_dataforseo_headers() -> dict:
    login    = get_ssm("DATAFORSEO_LOGIN")
    password = get_ssm("DATAFORSEO_PASSWORD")
    creds    = base64.b64encode(f"{login}:{password}".encode()).decode()
    return {
        "Authorization": f"Basic {creds}",
        "Content-Type":  "application/json",
    }


def check_rank(keyword: str, business_name: str, city: str, state: str, website_url: str) -> dict:
    headers = get_dataforseo_headers()

    # Build location string
    location = "United States"
    if city and state:
        location = f"{city},{state},United States"
    elif city:
        location = f"{city},United States"

    # Append city to keyword if not already there
    query = keyword
    if city and city.lower() not in keyword.lower():
        query = f"{keyword} {city}"

    payload = [{
        "keyword":       query,
        "location_name": location,
        "language_name": "English",
        "depth":         10,
        "se_domain":     "google.com",
    }]

    try:
        resp = requests.post(
            f"{DATAFORSEO_BASE}/serp/google/organic/live/advanced",
            headers=headers,
            json=payload,
            timeout=20,
        )
        data = resp.json()

        task = data.get("tasks", [{}])[0]
        if task.get("status_code") != 20000:
            log("warning", "dataforseo_error",
                status_code=task.get("status_code"),
                status_message=task.get("status_message"))
            return _fallback_response()

        items = task.get("result", [{}])[0].get("items", [])

    except Exception as e:
        log("error", "dataforseo_request_failed", error=str(e))
        return _fallback_response()

    # Parse results
    organic_rank = "Not in top 10"
    maps_rank    = "Not in Map Pack"
    found        = False

    for item in items:
        item_type = item.get("type", "")

        # Organic results
        if item_type == "organic":
            rank  = item.get("rank_absolute", 999)
            url   = item.get("url", "").lower()
            title = item.get("title", "").lower()

            name_match   = business_name.lower() in title
            domain_match = False
            if website_url:
                domain = website_url.lower().replace("https://", "").replace("http://", "").split("/")[0]
                domain_match = domain in url

            if (name_match or domain_match) and rank <= 10:
                organic_rank = rank
                found = True

        # Maps pack results
        if item_type in ("maps_pack", "local_pack"):
            for i, map_item in enumerate(item.get("items", []), 1):
                map_title = map_item.get("title", "").lower()
                if business_name.lower() in map_title:
                    maps_rank = i
                    found = True

    # Build message
    if found and organic_rank != "Not in top 10":
        if maps_rank != "Not in Map Pack":
            message = (
                f"{business_name} ranks #{organic_rank} on Google for {keyword} "
                f"and is in the Map Pack at position #{maps_rank}."
            )
        else:
            message = (
                f"{business_name} ranks #{organic_rank} on Google for {keyword} "
                f"— but you're not in the Map Pack."
            )
    elif found and maps_rank != "Not in Map Pack":
        message = (
            f"{business_name} is in the Map Pack at position #{maps_rank} for {keyword} "
            f"— but not in the top 10 organic results."
        )
    else:
        message = (
            f"{business_name} is not ranking in the top 10 for {keyword} "
            f"in {city or 'your area'}."
        )

    return {
        "organic_rank": organic_rank,
        "maps_rank":    maps_rank,
        "found":        found,
        "keyword":      keyword,
        "query_used":   query,
        "message":      message,
        "cta":          "See your full ranking report — enter your email below.",
    }


def _fallback_response() -> dict:
    return {
        "organic_rank": "Unknown",
        "maps_rank":    "Unknown",
        "found":        False,
        "keyword":      "",
        "query_used":   "",
        "message":      "We're having trouble checking rankings right now. Enter your email for your free full report.",
        "cta":          "Enter your email to get your free full ranking report.",
        "error":        "api_unavailable",
    }


# ----------------------------------------------------------------
# Core handler
# ----------------------------------------------------------------
def handle(payload: dict) -> dict:
    keyword       = (payload.get("keyword") or "").strip()
    business_name = (payload.get("business_name") or "").strip()
    city          = (payload.get("city") or "").strip()
    state         = (payload.get("state") or "").strip()
    website_url   = (payload.get("website_url") or "").strip()

    if not keyword:
        return {"status": "error", "code": "MISSING_KEYWORD", "message": "keyword is required"}
    if not business_name:
        return {"status": "error", "code": "MISSING_BUSINESS_NAME", "message": "business_name is required"}

    log("info", "rank_check_start", keyword=keyword, city=city, state=state)

    result = check_rank(keyword, business_name, city, state, website_url)

    log("info", "rank_check_complete",
        found=result.get("found"),
        organic_rank=str(result.get("organic_rank")),
        maps_rank=str(result.get("maps_rank")),
        version=CODE_VERSION)

    return {
        "status":       "success",
        "organic_rank": result["organic_rank"],
        "maps_rank":    result["maps_rank"],
        "found":        result["found"],
        "keyword":      result["keyword"],
        "message":      result["message"],
        "cta":          result["cta"],
        "agent":        "rankConfirmation",
        "version":      CODE_VERSION,
    }


# ----------------------------------------------------------------
# AgentCore entrypoint
# ----------------------------------------------------------------
if _SDK:
    app = BedrockAgentCoreApp()

    @app.entrypoint
    def rank_confirmation(payload: dict) -> dict:
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

    if __name__ == "__main__" and not _SDK:
        app.run()
