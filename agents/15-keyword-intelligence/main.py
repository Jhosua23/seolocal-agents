"""
================================================================
Agent 15 — Keyword Intelligence Generator
================================================================
AgentCore name  : keywordIntelligence
Phase           : 1 - Lead Generation
Sequence        : 15 of 15
Priority        : P1
Complexity      : Medium
Runtime         : Python 3.12 / ARM64
Version         : 1.0.0

Uses DataForSEO instead of SEMrush (same data, already in SSM).

SSM Parameters required:
  DATAFORSEO_LOGIN                  - SecureString
  DATAFORSEO_PASSWORD               - SecureString
  ANTHROPIC_API_KEY                 - SecureString
  CLAUDE_MODEL_ID                   - String
  GHL_API_KEY                       - SecureString
  GHL_PIPELINE_ID                   - String
  GHL_STAGE_KEYWORD_INTEL_DELIVERED - String (get from Chuck)
  S3_REPORTS_BUCKET                 - String
  RECURLY_WEBHOOK_SECRET            - SecureString (get from Chuck)

Trigger  : POST /keyword/intel
================================================================
"""

import base64
import hashlib
import hmac
import io
import json
import logging
import time
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
        "level": level,
        "event": event,
        "ts":    datetime.now(timezone.utc).isoformat(),
        "agent": "keywordIntelligence",
        "v":     "1.0.0",
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
GHL_BASE        = "https://services.leadconnectorhq.com"
LOCATION_ID     = "uXRl9WpDjS7LFjeYfQqD"
DATAFORSEO_BASE = "https://api.dataforseo.com/v3"

# Vertical keyword seeds — fallback when domain has no DataForSEO data
VERTICAL_SEEDS = {
    "hvac":     ["hvac repair", "air conditioning service", "heating repair", "furnace repair", "ac installation"],
    "plumbing": ["plumber near me", "plumbing repair", "drain cleaning", "water heater repair", "emergency plumber"],
    "dental":   ["dentist near me", "teeth cleaning", "dental implants", "emergency dentist", "cosmetic dentistry"],
    "legal":    ["attorney near me", "lawyer consultation", "personal injury attorney", "divorce lawyer", "criminal defense"],
    "roofing":  ["roof repair", "roofing contractor", "roof replacement", "storm damage roof", "roof inspection"],
    "default":  ["local business near me", "best service near me", "top rated", "affordable service", "professional service"],
}

TRANSACTIONAL_TERMS = {
    "near me", "service", "repair", "hire", "install",
    "replacement", "emergency", "affordable", "best", "top"
}


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


def _h(s: str) -> str:
    return hashlib.sha256(s.encode()).hexdigest()[:16]


# ----------------------------------------------------------------
# Recurly webhook validation
# ----------------------------------------------------------------
def validate_recurly_webhook(payload: dict) -> bool:
    try:
        secret = get_ssm("RECURLY_WEBHOOK_SECRET")
        if "pending" in secret.lower():
            log("warning", "recurly_secret_pending_skip_validation")
            return True
    except Exception:
        log("warning", "recurly_secret_not_found_skip_validation")
        return True

    signature = payload.get("recurly_signature", "")
    body       = payload.get("raw_body", "")
    if not signature or not body:
        return True

    expected = hmac.new(
        secret.encode(), body.encode(), hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(expected, signature)


# ----------------------------------------------------------------
# GHL helpers
# ----------------------------------------------------------------
def _ghl_headers() -> dict:
    return {
        "Authorization": f"Bearer {get_ssm('GHL_API_KEY')}",
        "Version":       "2021-07-28",
        "Content-Type":  "application/json",
    }


def ghl_search_contact(email: str) -> dict:
    resp = requests.get(
        f"{GHL_BASE}/contacts/search",
        headers=_ghl_headers(),
        params={"email": email, "locationId": LOCATION_ID},
        timeout=10,
    )
    if resp.status_code == 200:
        contacts = resp.json().get("contacts", [])
        return contacts[0] if contacts else {}
    log("warning", "ghl_search_failed",
        email_hash=_h(email), status=resp.status_code)
    return {}


def ghl_update_contact(contact_id: str, fields: list, tags: list, stage_id: str) -> bool:
    # Update custom fields
    resp = requests.put(
        f"{GHL_BASE}/contacts/{contact_id}",
        headers=_ghl_headers(),
        json={"customFields": fields},
        timeout=10,
    )
    ok = resp.status_code in (200, 201)
    if not ok:
        log("warning", "ghl_update_fields_failed",
            contact_id_hash=_h(contact_id), status=resp.status_code)

    # Apply tags
    requests.post(
        f"{GHL_BASE}/contacts/{contact_id}/tags",
        headers=_ghl_headers(),
        json={"tags": tags},
        timeout=10,
    )

    # Move pipeline stage
    if stage_id and "pending" not in stage_id.lower():
        pipeline_id = get_ssm("GHL_PIPELINE_ID")
        opp_resp = requests.get(
            f"{GHL_BASE}/opportunities/search",
            headers=_ghl_headers(),
            params={"contact_id": contact_id, "pipeline_id": pipeline_id},
            timeout=10,
        )
        if opp_resp.status_code == 200:
            opps = opp_resp.json().get("opportunities", [])
            if opps:
                opp_id = opps[0]["id"]
                requests.put(
                    f"{GHL_BASE}/opportunities/{opp_id}",
                    headers=_ghl_headers(),
                    json={"pipelineStageId": stage_id},
                    timeout=10,
                )
    return ok


def ghl_create_task(contact_id: str, title: str, body: str) -> None:
    from datetime import timedelta
    requests.post(
        f"{GHL_BASE}/contacts/{contact_id}/tasks",
        headers=_ghl_headers(),
        json={
            "title":      title,
            "body":       body,
            "contactId":  contact_id,
            "dueDate":    (datetime.now(timezone.utc) + timedelta(hours=4)).isoformat() + "Z",
            "completed":  False,
        },
        timeout=10,
    )


# ----------------------------------------------------------------
# DataForSEO helpers
# ----------------------------------------------------------------
def _dfs_headers() -> dict:
    login    = get_ssm("DATAFORSEO_LOGIN")
    password = get_ssm("DATAFORSEO_PASSWORD")
    creds    = base64.b64encode(f"{login}:{password}".encode()).decode()
    return {
        "Authorization": f"Basic {creds}",
        "Content-Type":  "application/json",
    }


def dfs_keywords_for_keywords(seeds: list, city: str, state: str) -> list:
    """Get related keywords with volume, difficulty, CPC from DataForSEO Labs."""
    location = "United States"
    if city and state:
        location = f"{city},{state},United States"
    elif city:
        location = f"{city},United States"

    payload = [{
        "keywords":      seeds[:10],
        "location_name": location,
        "language_name": "English",
        "limit":         50,
    }]

    try:
        resp = requests.post(
            f"{DATAFORSEO_BASE}/dataforseo_labs/google/keywords_for_keywords/live",
            headers=_dfs_headers(),
            json=payload,
            timeout=30,
        )
        data = resp.json()
        task = data.get("tasks", [{}])[0]
        if task.get("status_code") != 20000:
            log("warning", "dfs_keywords_error",
                status_code=task.get("status_code"),
                msg=task.get("status_message"))
            return []

        items    = task.get("result", [{}])[0].get("items", [])
        keywords = []
        for item in items:
            kw_data = item.get("keyword_data", {})
            kw_info = kw_data.get("keyword_info", {})
            keywords.append({
                "keyword":    item.get("keyword", ""),
                "volume":     kw_info.get("search_volume", 0) or 0,
                "difficulty": kw_data.get("keyword_properties", {}).get("keyword_difficulty", 50) or 50,
                "cpc":        kw_info.get("cpc", 0) or 0,
            })
        return keywords

    except Exception as e:
        log("error", "dfs_keywords_exception", error=str(e))
        return []


def dfs_serp_check(keyword: str, domain: str) -> dict:
    """Check current rank for a keyword and find top competitor."""
    payload = [{
        "keyword":       keyword,
        "location_code": 2840,
        "language_code": "en",
        "depth":         10,
        "se_domain":     "google.com",
    }]

    try:
        resp = requests.post(
            f"{DATAFORSEO_BASE}/serp/google/organic/live/advanced",
            headers=_dfs_headers(),
            json=payload,
            timeout=20,
        )
        data  = resp.json()
        task  = data.get("tasks", [{}])[0]
        if task.get("status_code") != 20000:
            return {"rank": None, "competitor": None}

        items          = task.get("result", [{}])[0].get("items", [])
        prospect_rank  = None
        top_competitor = None

        for item in items:
            if item.get("type") != "organic":
                continue
            url  = item.get("url", "").lower()
            rank = item.get("rank_absolute", 999)

            if domain.lower() in url and prospect_rank is None:
                prospect_rank = rank
            elif top_competitor is None and domain.lower() not in url:
                top_competitor = url.split("/")[2] if "/" in url else url

        return {"rank": prospect_rank, "competitor": top_competitor}

    except Exception as e:
        log("warning", "dfs_serp_exception", error=str(e))
        return {"rank": None, "competitor": None}


# ----------------------------------------------------------------
# Keyword scoring
# ----------------------------------------------------------------
def score_keyword(kw: dict, city: str, vertical: str) -> float:
    """
    opportunity_score = (volume x 0.4) + (difficulty_inverse x 0.3) + (intent x 0.3)
    """
    volume     = min(kw.get("volume", 0), 10000)
    difficulty = kw.get("difficulty", 50)
    keyword    = kw.get("keyword", "").lower()

    volume_score   = (volume / 10000) * 100
    difficulty_inv = 100 - difficulty
    intent_score   = 30  # informational default

    words = set(keyword.split())
    if words & TRANSACTIONAL_TERMS:
        intent_score = 100
    if city and city.lower() in keyword:
        intent_score = min(intent_score + 30, 100)
    if vertical and vertical.lower() in keyword:
        intent_score = min(intent_score + 20, 100)

    return (volume_score * 0.4) + (difficulty_inv * 0.3) + (intent_score * 0.3)


def build_keyword_seeds(business_name: str, city: str, vertical: str) -> list:
    """Build seed keywords from business context."""
    vertical_lower = (vertical or "").lower()
    seeds = []

    for key, kws in VERTICAL_SEEDS.items():
        if key in vertical_lower or key in business_name.lower():
            seeds = [f"{kw} {city}".strip() for kw in kws]
            break

    if not seeds:
        seeds = [f"{kw} {city}".strip() for kw in VERTICAL_SEEDS["default"]]

    if city:
        seeds.append(f"best {vertical_lower} {city}".strip() if vertical_lower else f"best service {city}")
        seeds.append(f"{vertical_lower} near me {city}".strip() if vertical_lower else f"service near me {city}")

    return list(dict.fromkeys(seeds))[:10]  # dedupe, max 10


# ----------------------------------------------------------------
# Claude narrative
# ----------------------------------------------------------------
def generate_narrative(
    business_name: str,
    city: str,
    top_keywords: list,
    keyword_gap_count: int,
) -> str:
    try:
        model_id = get_ssm("CLAUDE_MODEL_ID")
        api_key  = get_ssm("ANTHROPIC_API_KEY")
        top_3    = [kw["keyword"] for kw in top_keywords[:3]]

        prompt = (
            f"You are an SEO analyst writing a keyword opportunity report for "
            f"{business_name} in {city}.\n\n"
            f"They have {keyword_gap_count} keyword opportunities. "
            f"Top 3 priority keywords:\n"
            f"1. {top_3[0] if len(top_3) > 0 else 'local service keywords'}\n"
            f"2. {top_3[1] if len(top_3) > 1 else 'near me searches'}\n"
            f"3. {top_3[2] if len(top_3) > 2 else 'competitor keywords'}\n\n"
            f"Write exactly 2 paragraphs:\n"
            f"Paragraph 1: What these keyword gaps mean for their business "
            f"(lost customers, revenue impact).\n"
            f"Paragraph 2: Why these 3 keywords should be targeted first "
            f"and what winning them would mean.\n\n"
            f"Keep it specific, urgent, business-focused. Max 150 words total."
        )

        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key":         api_key,
                "anthropic-version": "2023-06-01",
                "content-type":      "application/json",
            },
            json={
                "model":      model_id,
                "max_tokens": 300,
                "messages":   [{"role": "user", "content": prompt}],
            },
            timeout=30,
        )

        if resp.status_code == 200:
            return resp.json()["content"][0]["text"]

    except Exception as e:
        log("warning", "claude_narrative_failed", error=str(e))

    return (
        f"{business_name} is missing significant local search traffic in {city}. "
        f"With {keyword_gap_count} keyword opportunities identified, competitors are "
        f"capturing customers actively searching for your services right now.\n\n"
        f"The top priority keywords represent high-intent searches where prospects are "
        f"ready to buy. Targeting these first delivers the fastest ROI and establishes "
        f"your authority in the most competitive local search positions. "
        f"SEO Local's Year 1 plan targets all {keyword_gap_count} of these opportunities."
    )


# ----------------------------------------------------------------
# PDF generation
# ----------------------------------------------------------------
def generate_pdf(
    business_name: str,
    city: str,
    state: str,
    top_keywords: list,
    narrative: str,
    keyword_gap_count: int,
) -> bytes:
    try:
        from reportlab.lib import colors
        from reportlab.lib.pagesizes import letter
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.lib.units import inch
        from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle

        buffer = io.BytesIO()
        doc    = SimpleDocTemplate(
            buffer, pagesize=letter,
            topMargin=0.75*inch, bottomMargin=0.75*inch,
            leftMargin=0.75*inch, rightMargin=0.75*inch,
        )
        styles = getSampleStyleSheet()
        story  = []

        DARK   = colors.HexColor("#1a1a2e")
        LIGHT  = colors.HexColor("#f5f5f5")
        ACCENT = colors.HexColor("#e8f4fd")

        title_style = ParagraphStyle("Title2", parent=styles["Title"],
            fontSize=22, textColor=DARK, spaceAfter=6)
        sub_style   = ParagraphStyle("Sub", parent=styles["Normal"],
            fontSize=11, textColor=colors.HexColor("#555555"), spaceAfter=20)
        body_style  = ParagraphStyle("Body", parent=styles["Normal"],
            fontSize=10, leading=14, textColor=colors.HexColor("#333333"), spaceAfter=12)
        h2_style    = ParagraphStyle("H2", parent=styles["Heading2"],
            fontSize=14, textColor=DARK, spaceBefore=16, spaceAfter=8)

        # Header
        story.append(Paragraph("Keyword Intelligence Report", title_style))
        story.append(Paragraph(
            f"{business_name} — {city}, {state}  |  "
            f"Generated {datetime.now().strftime('%B %d, %Y')}",
            sub_style
        ))
        story.append(Paragraph(
            f"<b>{keyword_gap_count} keyword opportunities</b> identified for your market.",
            body_style
        ))

        # Narrative
        story.append(Paragraph("Your Keyword Opportunity", h2_style))
        for para in narrative.split("\n\n"):
            if para.strip():
                story.append(Paragraph(para.strip(), body_style))

        story.append(Spacer(1, 0.2*inch))

        # Keywords table
        story.append(Paragraph("Keywords Worth Owning — Top 20", h2_style))
        table_data = [["#", "Keyword", "Monthly Searches", "Difficulty", "Your Rank", "Opp. Score"]]
        for i, kw in enumerate(top_keywords[:20], 1):
            rank     = str(kw.get("current_rank") or "Not ranking")
            vol      = f"{kw.get('volume', 0):,}"
            diff     = kw.get("difficulty", 50)
            opp      = f"{kw.get('opportunity_score', 0):.0f}"
            diff_str = "Low" if diff < 30 else "Medium" if diff < 60 else "High"
            table_data.append([str(i), kw["keyword"], vol, diff_str, rank, opp])

        col_widths = [0.3*inch, 2.4*inch, 1.2*inch, 0.9*inch, 1.0*inch, 0.9*inch]
        table = Table(table_data, colWidths=col_widths, repeatRows=1)
        table.setStyle(TableStyle([
            ("BACKGROUND",    (0, 0), (-1, 0),  DARK),
            ("TEXTCOLOR",     (0, 0), (-1, 0),  colors.white),
            ("FONTNAME",      (0, 0), (-1, 0),  "Helvetica-Bold"),
            ("FONTSIZE",      (0, 0), (-1, 0),  9),
            ("ALIGN",         (0, 0), (-1, -1), "CENTER"),
            ("ALIGN",         (1, 0), (1, -1),  "LEFT"),
            ("FONTSIZE",      (0, 1), (-1, -1), 8),
            ("ROWBACKGROUNDS",(0, 1), (-1, -1), [colors.white, LIGHT]),
            ("GRID",          (0, 0), (-1, -1), 0.5, colors.HexColor("#dddddd")),
            ("TOPPADDING",    (0, 0), (-1, -1), 4),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
            ("BACKGROUND",    (0, 1), (-1, 3),  ACCENT),
            ("FONTNAME",      (0, 1), (-1, 3),  "Helvetica-Bold"),
        ]))
        story.append(table)

        # Top 3 detail
        story.append(Spacer(1, 0.2*inch))
        story.append(Paragraph("Top 3 Priority Keywords", h2_style))
        for i, kw in enumerate(top_keywords[:3], 1):
            diff     = kw.get("difficulty", 50)
            diff_str = "Low" if diff < 30 else "Medium" if diff < 60 else "High"
            story.append(Paragraph(
                f"<b>#{i} — {kw['keyword']}</b><br/>"
                f"Monthly searches: {kw.get('volume', 0):,} &nbsp;|&nbsp; "
                f"Difficulty: {diff_str} ({diff}) &nbsp;|&nbsp; "
                f"Your current rank: {kw.get('current_rank') or 'Not ranking'}<br/>"
                f"Top competitor: {kw.get('top_competitor') or 'Unknown'}",
                body_style
            ))

        # CTA
        story.append(Spacer(1, 0.3*inch))
        cta_style = ParagraphStyle("CTA", parent=styles["Normal"],
            fontSize=12, textColor=colors.white,
            backColor=DARK, borderPadding=12, leading=18)
        story.append(Paragraph(
            "Your SEO Local plan targets all of these keywords in Year 1. "
            "Book your strategy call to see your custom ranking roadmap.",
            cta_style
        ))

        doc.build(story)
        return buffer.getvalue()

    except Exception as e:
        log("error", "pdf_generation_failed", error=str(e))
        # Minimal fallback PDF
        return (
            b"%PDF-1.4\n1 0 obj<</Type/Catalog/Pages 2 0 R>>endobj "
            b"2 0 obj<</Type/Pages/Kids[3 0 R]/Count 1>>endobj "
            b"3 0 obj<</Type/Page/MediaBox[0 0 612 792]/Parent 2 0 R>>endobj\n"
            b"xref\n0 4\n0000000000 65535 f\n0000000009 00000 n\n"
            b"0000000058 00000 n\n0000000115 00000 n\n"
            b"trailer<</Size 4/Root 1 0 R>>\nstartxref\n190\n%%EOF"
        )


# ----------------------------------------------------------------
# S3 upload
# ----------------------------------------------------------------
def upload_to_s3(pdf_bytes: bytes, contact_id: str) -> str:
    s3     = boto3.client("s3", region_name=REGION)
    bucket = get_ssm("S3_REPORTS_BUCKET")
    date   = datetime.now().strftime("%Y%m%d")
    key    = f"keyword-intel/{contact_id}-{date}.pdf"

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=pdf_bytes,
        ContentType="application/pdf",
    )

    url = s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": bucket, "Key": key},
        ExpiresIn=72 * 3600,  # 72 hours
    )
    log("info", "pdf_uploaded", key=key, bucket=bucket)
    return url


# ----------------------------------------------------------------
# Core handler
# ----------------------------------------------------------------
def handle(payload: dict) -> dict:
    now = datetime.now(timezone.utc)

    # --- Step 1: Validate Recurly webhook ---
    if not validate_recurly_webhook(payload):
        log("error", "invalid_recurly_signature")
        return {"status": "error", "code": "INVALID_SIGNATURE"}

    # --- Extract fields from Recurly or direct payload ---
    invoice      = payload.get("data", {}).get("invoice", {})
    account      = invoice.get("account", {})
    email        = (account.get("email") or payload.get("email") or "").strip()
    account_code = (account.get("account_code") or payload.get("contact_id") or "").strip()

    if not email:
        return {"status": "error", "code": "MISSING_EMAIL"}

    log("info", "keyword_intel_start", email_hash=_h(email))

    # --- Step 2: Look up GHL contact ---
    contact    = ghl_search_contact(email)
    contact_id = contact.get("id") or account_code

    if not contact_id:
        log("error", "contact_not_found", email_hash=_h(email))
        return {"status": "error", "code": "CONTACT_NOT_FOUND"}

    # Extract contact data
    business_name = (contact.get("companyName") or payload.get("business_name") or "").strip()
    city          = (contact.get("city") or payload.get("city") or "").strip()
    state         = (contact.get("state") or payload.get("state") or "").strip()
    vertical      = (payload.get("vertical") or "").strip()
    website_url   = (contact.get("website") or payload.get("website_url") or "").strip()

    domain = ""
    if website_url:
        domain = website_url.replace("https://", "").replace("http://", "").split("/")[0]

    if not business_name:
        return {"status": "error", "code": "MISSING_BUSINESS_NAME"}

    log("info", "contact_found",
        contact_id_hash=_h(contact_id),
        business_name=business_name,
        city=city, has_domain=bool(domain))

    # --- Step 3: Build seed keywords ---
    seeds = build_keyword_seeds(business_name, city, vertical)
    log("info", "seeds_built", count=len(seeds))

    # --- Step 4: Get keywords from DataForSEO ---
    raw_keywords = dfs_keywords_for_keywords(seeds, city, state)
    log("info", "dfs_keywords_returned", count=len(raw_keywords))

    # Fallback if DataForSEO returns nothing
    if not raw_keywords:
        log("warning", "dfs_no_data_using_seeds_as_fallback")
        raw_keywords = [
            {"keyword": s, "volume": 100, "difficulty": 40, "cpc": 5.0}
            for s in seeds
        ]

    # --- Step 5: Filter keywords with volume >= 50 ---
    filtered = [kw for kw in raw_keywords if kw.get("volume", 0) >= 50]
    if not filtered:
        filtered = raw_keywords  # relax filter if nothing passes

    # --- Step 6: Score and rank keywords ---
    for kw in filtered:
        kw["opportunity_score"] = score_keyword(kw, city, vertical)

    filtered.sort(key=lambda x: x["opportunity_score"], reverse=True)
    top_keywords      = filtered[:20]
    keyword_gap_count = len(filtered)

    # --- Step 7: Check current ranks for top 5 keywords ---
    if domain:
        log("info", "checking_ranks", keyword_count=min(5, len(top_keywords)))
        for kw in top_keywords[:5]:
            rank_data             = dfs_serp_check(kw["keyword"], domain)
            kw["current_rank"]    = rank_data.get("rank")
            kw["top_competitor"]  = rank_data.get("competitor")
            time.sleep(0.2)

    # --- Step 8: Generate Claude narrative ---
    log("info", "generating_narrative")
    narrative = generate_narrative(business_name, city, top_keywords, keyword_gap_count)

    # --- Step 9: Generate PDF ---
    log("info", "generating_pdf")
    pdf_bytes = generate_pdf(
        business_name, city, state,
        top_keywords, narrative, keyword_gap_count,
    )

    # --- Step 10: Upload PDF to S3 ---
    log("info", "uploading_to_s3")
    try:
        report_url = upload_to_s3(pdf_bytes, contact_id)
    except Exception as e:
        log("error", "s3_upload_failed", error=str(e))
        ghl_create_task(
            contact_id,
            "URGENT — Keyword Intel report upload failed",
            f"PDF generated but S3 upload failed. Error: {str(e)}. Manual delivery needed.",
        )
        return {"status": "error", "code": "S3_UPLOAD_FAILED", "message": str(e)}

    # --- Step 11: Update GHL contact ---
    top_keyword = top_keywords[0]["keyword"] if top_keywords else ""
    fields = [
        {"key": "keyword_intel_report_url", "field_value": report_url},
        {"key": "keyword_gap_count",        "field_value": str(keyword_gap_count)},
        {"key": "top_opportunity_keyword",  "field_value": top_keyword},
        {"key": "keyword_intel_date",       "field_value": now.isoformat()},
    ]
    tags = ["keyword-intel-delivered"]

    try:
        stage_id = get_ssm("GHL_STAGE_KEYWORD_INTEL_DELIVERED")
    except Exception:
        stage_id = ""
        log("warning", "stage_id_missing_skipping_stage_move")

    ghl_update_contact(contact_id, fields, tags, stage_id)

    log("info", "keyword_intel_complete",
        contact_id_hash=_h(contact_id),
        keyword_gap_count=keyword_gap_count,
        top_keyword=top_keyword,
        pdf_size_kb=len(pdf_bytes) // 1024,
        version=CODE_VERSION)

    return {
        "status":                  "success",
        "contact_id":              contact_id,
        "keyword_gap_count":       keyword_gap_count,
        "top_opportunity_keyword": top_keyword,
        "report_url":              report_url,
        "keywords_generated":      len(top_keywords),
        "agent":                   "keywordIntelligence",
        "version":                 CODE_VERSION,
    }


# ----------------------------------------------------------------
# AgentCore entrypoint
# ----------------------------------------------------------------
if _SDK:
    app = BedrockAgentCoreApp()

    @app.entrypoint
    def keyword_intelligence(payload: dict) -> dict:
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

    app.run()
