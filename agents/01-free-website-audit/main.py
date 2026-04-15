# SEO Local - Agent 01 - Free Website Audit - v1.1
from typing import Optional
import json
import os
import re
from datetime import datetime

import requests
from bs4 import BeautifulSoup

# ─────────────────────────────────────────────────────────────────────────────
# When running on AWS AgentCore, we import the real SDK.
# Locally, we mock it so you can test without AWS.
# ─────────────────────────────────────────────────────────────────────────────
try:
    import boto3
    from bedrock_agentcore.runtime import BedrockAgentCoreApp
    USE_AWS = True
except ImportError:
    USE_AWS = False

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG — reads from environment variables for local testing.
# For AWS deployment, these come from SSM Parameter Store.
# Never hardcode keys in production.
# ─────────────────────────────────────────────────────────────────────────────
LOCAL_CONFIG = {
    "PAGESPEED_API_KEY":  os.environ.get("PAGESPEED_API_KEY", ""),
    "GHL_API_KEY":        os.environ.get("GHL_API_KEY", ""),
    "GHL_PIPELINE_ID":    os.environ.get("GHL_PIPELINE_ID", "xEK3qOXCRezO6aoHN6AS"),
    "GHL_STAGE_NEW_LEAD": os.environ.get("GHL_STAGE_NEW_LEAD", "993f30f0-5417-4a97-a622-2e8e0c376a32"),
    "LOCATION_ID":        os.environ.get("LOCATION_ID", "uXRl9WpDjS7LFjeYfQqD"),
}

# ─────────────────────────────────────────────────────────────────────────────
# SSM helper — uses real SSM on AWS, falls back to LOCAL_CONFIG locally
# ─────────────────────────────────────────────────────────────────────────────
def get_config(key: str) -> str:
    if USE_AWS:
        ssm = boto3.client("ssm", region_name="us-east-1")
        return ssm.get_parameter(Name=key, WithDecryption=True)["Parameter"]["Value"]
    return LOCAL_CONFIG.get(key, "")

# ─────────────────────────────────────────────────────────────────────────────
# GHL headers helper
# ─────────────────────────────────────────────────────────────────────────────
def ghl_headers(api_key: str) -> dict:
    return {
        "Authorization": f"Bearer {api_key}",
        "Version": "2021-07-28",
        "Content-Type": "application/json",
    }

# ─────────────────────────────────────────────────────────────────────────────
# PAGE SPEED — Check 01 & 02
# ─────────────────────────────────────────────────────────────────────────────
def check_pagespeed(url: str, api_key: str) -> tuple:
    """Returns (mobile_score, desktop_score). Returns (-1, -1) on failure."""
    if not api_key:
        print("  [PageSpeed] No API key — skipping checks 01 & 02")
        return -1, -1
    try:
        base = "https://www.googleapis.com/pagespeedonline/v5/runPagespeed"
        print("  [PageSpeed] Checking mobile speed...")
        mobile_r = requests.get(
            f"{base}?url={url}&strategy=mobile&key={api_key}",
            timeout=30
        ).json()
        print("  [PageSpeed] Checking desktop speed...")
        desktop_r = requests.get(
            f"{base}?url={url}&strategy=desktop&key={api_key}",
            timeout=30
        ).json()
        m = int(mobile_r["lighthouseResult"]["categories"]["performance"]["score"] * 100)
        d = int(desktop_r["lighthouseResult"]["categories"]["performance"]["score"] * 100)
        print(f"  [PageSpeed] Mobile: {m} | Desktop: {d}")
        return m, d
    except Exception as e:
        print(f"  [PageSpeed] Failed: {e}")
        return -1, -1

def score_pagespeed(score: int) -> int:
    if score == -1: return 0
    if score >= 90: return 10
    if score >= 70: return 7
    if score >= 50: return 4
    return 0

# ─────────────────────────────────────────────────────────────────────────────
# 13-CHECK SCORING ENGINE
# ─────────────────────────────────────────────────────────────────────────────
def run_checks(url: str, html: str, pagespeed_api_key: str) -> list:
    soup = BeautifulSoup(html, "html.parser")
    page_text = soup.get_text()
    checks = []

    # Check 01 — Mobile Page Speed
    mobile_score, desktop_score = check_pagespeed(url, pagespeed_api_key)
    ps_failed = mobile_score == -1
    checks.append({
        "id": "01", "name": "Page Speed (Mobile)",
        "score": score_pagespeed(mobile_score),
        "raw_value": f"{mobile_score}/100" if not ps_failed else "Pending",
        "issue": "Page speed failing on mobile — you're losing visitors before they read a word",
        "pending": ps_failed,
    })

    # Check 02 — Desktop Page Speed
    checks.append({
        "id": "02", "name": "Page Speed (Desktop)",
        "score": score_pagespeed(desktop_score),
        "raw_value": f"{desktop_score}/100" if not ps_failed else "Pending",
        "issue": "Page speed failing on desktop — user experience is degraded",
        "pending": ps_failed,
    })

    # Check 03 — HTTPS/SSL
    is_https = url.startswith("https://")
    checks.append({
        "id": "03", "name": "HTTPS / SSL",
        "score": 10 if is_https else 0,
        "raw_value": "HTTPS" if is_https else "HTTP only",
        "issue": "No SSL certificate — browsers flag your site as Not Secure",
    })

    # Check 04 — Title Tag
    title_tag = soup.find("title")
    title_text = title_tag.get_text(strip=True) if title_tag else ""
    title_len = len(title_text)
    if title_tag and 50 <= title_len <= 60:
        t_score = 10
    elif title_tag and title_text:
        t_score = 5
    else:
        t_score = 0
    checks.append({
        "id": "04", "name": "Title Tag",
        "score": t_score,
        "raw_value": f'"{title_text[:50]}..." ({title_len} chars)' if title_text else "Missing",
        "issue": "Title tag missing or not optimized (should be 50-60 chars with keyword)",
    })

    # Check 05 — Meta Description
    meta_desc = soup.find("meta", attrs={"name": "description"})
    desc_text = meta_desc.get("content", "") if meta_desc else ""
    desc_len = len(desc_text)
    if meta_desc and 140 <= desc_len <= 160:
        d_score = 10
    elif meta_desc and desc_text:
        d_score = 5
    else:
        d_score = 0
    checks.append({
        "id": "05", "name": "Meta Description",
        "score": d_score,
        "raw_value": f"{desc_len} chars" if desc_text else "Missing",
        "issue": "Meta description missing or off-spec (should be 140-160 chars)",
    })

    # Check 06 — H1 Tag
    h1 = soup.find("h1")
    h1_text = h1.get_text(strip=True) if h1 else ""
    checks.append({
        "id": "06", "name": "H1 Tag",
        "score": 10 if h1_text else 0,
        "raw_value": f'"{h1_text[:40]}"' if h1_text else "Missing",
        "issue": "H1 tag missing — search engines can not identify your main topic",
    })

    # Check 07 — Schema Markup
    schema_tags = soup.find_all("script", attrs={"type": "application/ld+json"})
    schema_text = " ".join([t.get_text() for t in schema_tags]).lower()
    if "localbusiness" in schema_text:
        s_score, s_val = 10, "LocalBusiness schema found"
    elif "organization" in schema_text:
        s_score, s_val = 5, "Organization schema only"
    else:
        s_score, s_val = 0, "No schema found"
    checks.append({
        "id": "07", "name": "Schema Markup",
        "score": s_score,
        "raw_value": s_val,
        "issue": "No schema markup — Google can not identify your business type",
    })

    # Check 08 — Google Business Profile Link
    gbp_link = soup.find("a", href=re.compile(r"google\.com/maps|maps\.google|g\.page"))
    checks.append({
        "id": "08", "name": "Google Business Profile Link",
        "score": 10 if gbp_link else 0,
        "raw_value": "GBP link found" if gbp_link else "Not found",
        "issue": "No Google Business Profile link — missing trust signal for local search",
    })

    # Check 09 — NAP (Name, Address, Phone)
    has_phone = bool(re.search(r"\(?\d{3}\)?[\s\-\.]\d{3}[\s\-\.]\d{4}", page_text))
    has_address = bool(re.search(
        r"\d+\s+\w+\s+(st|ave|rd|blvd|dr|ln|way|ct|street|avenue|road|boulevard|drive|lane|court)",
        page_text, re.I
    ))
    if has_phone and has_address:
        nap_score, nap_val = 10, "Name, address & phone found"
    elif has_phone:
        nap_score, nap_val = 5, "Phone found, address missing"
    else:
        nap_score, nap_val = 0, "Phone & address missing"
    checks.append({
        "id": "09", "name": "NAP Consistency",
        "score": nap_score,
        "raw_value": nap_val,
        "issue": "Phone/address not visible on page — hurts local rankings",
    })

    # Check 10 — Mobile Responsive
    viewport = soup.find("meta", attrs={"name": "viewport"})
    checks.append({
        "id": "10", "name": "Mobile Responsive",
        "score": 10 if viewport else 0,
        "raw_value": viewport.get("content", "")[:40] if viewport else "Missing",
        "issue": "No viewport meta tag — site is not mobile-friendly",
    })

    # Check 11 — Contact Form
    form = soup.find("form")
    checks.append({
        "id": "11", "name": "Contact Form",
        "score": 10 if form else 0,
        "raw_value": "Form found" if form else "No form found",
        "issue": "No contact form — visitors can not easily reach you",
    })

    # Check 12 — Reviews / Social Proof
    review_keywords = [
        "testimonial", "review", "stars", "rating",
        "rated", "customer said", "client said", "5 star"
    ]
    has_reviews = any(kw in page_text.lower() for kw in review_keywords)
    checks.append({
        "id": "12", "name": "Reviews / Social Proof",
        "score": 10 if has_reviews else 0,
        "raw_value": "Social proof found" if has_reviews else "Not found",
        "issue": "No reviews or social proof — trust signals are missing",
    })

    # Check 13 — Content Freshness
    date_pattern = re.search(r"202[4-6]", page_text)
    checks.append({
        "id": "13", "name": "Content Freshness",
        "score": 10 if date_pattern else 0,
        "raw_value": f"Year {date_pattern.group()} found" if date_pattern else "No recent date found",
        "issue": "No recent content found — site may appear stale to search engines",
    })

    return checks

# ─────────────────────────────────────────────────────────────────────────────
# GHL — Create contact then create opportunity separately
# ─────────────────────────────────────────────────────────────────────────────
def create_ghl_contact(
    data: dict,
    api_key: str,
    pipeline_id: str,
    stage_id: str,
    location_id: str
) -> Optional[str]:
    if not api_key:
        print("  [GHL] No API key — skipping contact creation")
        return None
    try:
        # Step 1 — Create contact (no pipeline fields here)
        contact_payload = {
            "locationId":  location_id,
            "firstName":   data.get("first_name", "Website"),
            "email":       data.get("email", ""),
            "phone":       data.get("phone", ""),
            "companyName": data.get("business_name", ""),
            "source":      "landing-page-audit",
            "tags":        ["audit-completed"],
            "customFields": [
                {"key": "your_website",  "field_value": data["website_url"]},
                {"key": "audit_score",   "field_value": str(data["audit_score"])},
                {"key": "audit_grade",   "field_value": data["audit_grade"]},
                {"key": "audit_date",    "field_value": datetime.utcnow().isoformat() + "Z"},
            ],
        }
        resp = requests.post(
            "https://services.leadconnectorhq.com/contacts/",
            headers=ghl_headers(api_key),
            json=contact_payload,
            timeout=10,
        )
        if resp.status_code not in (200, 201):
            print(f"  [GHL] Contact failed: {resp.status_code} — {resp.text[:200]}")
            return None

        contact_id = resp.json().get("contact", {}).get("id")
        print(f"  [GHL] Contact created: {contact_id}")

        # Step 2 — Create opportunity with pipeline stage
        opp_payload = {
            "pipelineId":      pipeline_id,
            "pipelineStageId": stage_id,
            "contactId":       contact_id,
            "locationId":      location_id,
            "name":            f"{data.get('business_name', 'Website Audit')} — SEO Local",
            "status":          "open",
            "monetaryValue":   797,
        }
        opp_resp = requests.post(
            "https://services.leadconnectorhq.com/opportunities/",
            headers=ghl_headers(api_key),
            json=opp_payload,
            timeout=10,
        )
        if opp_resp.status_code in (200, 201):
            opp_id = opp_resp.json().get("opportunity", {}).get("id")
            print(f"  [GHL] Opportunity created: {opp_id}")
        else:
            print(f"  [GHL] Opportunity failed: {opp_resp.status_code} — {opp_resp.text[:200]}")

        return contact_id

    except Exception as e:
        print(f"  [GHL] Exception: {e}")
        return None

# ─────────────────────────────────────────────────────────────────────────────
# CORE AGENT FUNCTION — same logic runs locally and on AWS
# ─────────────────────────────────────────────────────────────────────────────
def run_audit(payload: dict) -> dict:
    # Load config
    pagespeed_api_key = get_config("PAGESPEED_API_KEY")
    ghl_api_key       = get_config("GHL_API_KEY")
    pipeline_id       = get_config("GHL_PIPELINE_ID")
    stage_id          = get_config("GHL_STAGE_NEW_LEAD")
    location_id       = get_config("LOCATION_ID")

    website_url   = payload.get("website_url", "").strip()
    business_name = payload.get("business_name", "")
    email         = payload.get("email", "")
    first_name    = payload.get("first_name", "")
    phone         = payload.get("phone", "")

    if not website_url:
        return {"error": "missing_url"}

    # Add https:// if missing
    if not website_url.startswith("http"):
        website_url = "https://" + website_url

    print(f"\n[Audit] Checking: {website_url}")

    # Step 1 — Check site is reachable
    print("[Audit] Step 1: Checking if site is reachable...")
    try:
        head_resp = requests.head(website_url, timeout=8, allow_redirects=True)
        if head_resp.status_code >= 400:
            return {"error": "site_unreachable", "status_code": head_resp.status_code}
        print(f"  Site is reachable (HTTP {head_resp.status_code})")
    except Exception as e:
        print(f"  Site unreachable: {e}")
        return {"error": "site_unreachable"}

    # Step 2 — Fetch HTML
    print("[Audit] Step 2: Fetching HTML...")
    try:
        html_resp = requests.get(
            website_url, timeout=12,
            headers={"User-Agent": "Mozilla/5.0"}
        )
        html = html_resp.text
        print(f"  HTML fetched ({len(html)} chars)")
    except Exception as e:
        print(f"  HTML fetch failed: {e}")
        return {"error": "site_unreachable"}

    # Step 3 — Run 13 checks
    print("[Audit] Step 3: Running 13 checks...")
    checks = run_checks(website_url, html, pagespeed_api_key)

    # Step 4 — Calculate score and grade
    total_score = sum(c["score"] for c in checks)
    max_score   = 130
    grade = (
        "A" if total_score >= 110 else
        "B" if total_score >= 85 else
        "C" if total_score >= 55 else
        "D"
    )
    print(f"[Audit] Score: {total_score}/{max_score} — Grade: {grade}")

    # Step 5 — Top 3 issues (lowest scoring)
    sorted_checks = sorted(checks, key=lambda c: c["score"])
    top_3_issues = [
        {"check": c["name"], "issue": c["issue"], "score": c["score"]}
        for c in sorted_checks[:3]
    ]

    # Step 6 — Create GHL contact + opportunity
    print("[Audit] Step 6: Creating GHL contact...")
    create_ghl_contact(
        {
            "website_url":   website_url,
            "business_name": business_name,
            "email":         email,
            "first_name":    first_name,
            "phone":         phone,
            "audit_score":   total_score,
            "audit_grade":   grade,
        },
        ghl_api_key, pipeline_id, stage_id, location_id,
    )

    # Step 7 — Return result
    return {
        "status":        "success",
        "score":         total_score,
        "max_score":     max_score,
        "grade":         grade,
        "business_name": business_name,
        "website_url":   website_url,
        "top_3_issues":  top_3_issues,
        "all_checks":    checks,
        "cta":           "Book your free strategy call to see your full 13-point report, competitor comparison, and 90-day fix roadmap.",
        "agent":         "freeWebsiteAudit",
        "version":       "1.1",
    }

# ─────────────────────────────────────────────────────────────────────────────
# AWS AgentCore entrypoint (only active when deployed)
# ─────────────────────────────────────────────────────────────────────────────
if USE_AWS:
    app = BedrockAgentCoreApp()

    @app.entrypoint
    def free_website_audit(payload: dict) -> dict:
        return run_audit(payload)
    app.run()

# ─────────────────────────────────────────────────────────────────────────────
# LOCAL TEST RUNNER — python main.py
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__" and not USE_AWS:
    print("=== SEO Local - Free Website Audit ===")
    website = input("Enter website URL: ").strip()
    email   = input("Enter your email: ").strip()
    fname   = input("Enter your first name: ").strip()
    biz     = input("Enter business name: ").strip()
    phone   = input("Enter phone number (optional, press Enter to skip): ").strip()

    test_payload = {
        "website_url":   website,
        "email":         email,
        "first_name":    fname,
        "business_name": biz,
        "phone":         phone,
    }

    result = run_audit(test_payload)

    print("\n" + "="*60)
    print("AUDIT RESULT")
    print("="*60)
    print(f"Website:  {result.get('website_url')}")
    print(f"Score:    {result.get('score')} / {result.get('max_score')}")
    print(f"Grade:    {result.get('grade')}")
    print()
    print("TOP 3 ISSUES (what the prospect sees):")
    for i, issue in enumerate(result.get("top_3_issues", []), 1):
        print(f"  {i}. {issue['issue']}  [score: {issue['score']}/10]")
    print()
    print("ALL 13 CHECKS (full breakdown):")
    for c in result.get("all_checks", []):
        status  = "✓" if c["score"] >= 7 else "~" if c["score"] >= 4 else "✗"
        pending = " (Pending)" if c.get("pending") else ""
        print(f"  {status} Check {c['id']}: {c['name']:30s} {c['score']:2d}/10  [{c.get('raw_value','')}]{pending}")
    print()
    print("Full JSON output saved to: result.json")

    with open("result.json", "w") as f:
        json.dump(result, f, indent=2)

<<<<<<< HEAD
=======
    if USE_AWS:
        app.run()
>>>>>>> 2a57ca2883405f6e18baedefbf5e180b9bdb9ac3
