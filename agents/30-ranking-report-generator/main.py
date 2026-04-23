"""
================================================================
Agent 30 — Ranking Report Generator
================================================================
AgentCore name  : rankingReportGenerator
Phase           : 2 — Conversion & Retention Engine
Module          : 1 — Lead Acquisition (Module 1 Addition)
Wave            : 4
Priority        : P1
Runtime         : Python 3.12 / ARM64
Version         : 1.0.0

NO RDS — ranking data stored in GHL custom fields.
Dedup  : tag free-report-delivered on contact.

WHAT IT DOES:
  1. Validates payload + derives keywords if missing
  2. Builds 5-keyword set (primary + 4 vertical related)
  3. Calls DataForSEO Live SERP for each keyword in parallel
  4. Identifies top 3 competitors from SERP results
  5. Calculates ranking gap score
  6. Estimates traffic opportunity at positions 1, 3, 10
  7. Calls Claude to generate plain-English narrative
  8. Builds branded PDF (6 sections) via reportlab
  9. Uploads PDF to S3
  10. Sends delivery email via GHL with PDF link
  11. Updates GHL custom fields (DELIVERABLE_URL, etc.)
  12. Applies GHL tags (free-report-delivered, $49-offer-eligible)
  13. Moves GHL stage to Paid Report Delivered
  14. Logs to CloudWatch

================================================================
SSM Parameters:
  Already in your SSM:
    GHL_API_KEY                   - SecureString
    DATAFORSEO_LOGIN              - String
    DATAFORSEO_PASSWORD           - SecureString
    CLAUDE_MODEL_ID               - String
    GHL_STAGE_PAID_REPORT_DELIVERED - String

  Ask Chuck to add:
    ANTHROPIC_API_KEY             - SecureString
    S3_REPORTS_BUCKET             - String
    RECURLY_DEEP_DIVE_CHECKOUT_URL - String
    GHL_LOCATION_ID               - String (if not present)
================================================================
"""

import base64
import concurrent.futures
import hashlib
import io
import json
import logging
import re
import time
from datetime import datetime, timezone

import boto3
import requests

# reportlab — PDF generation
try:
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import letter
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import inch
    from reportlab.platypus import (
        SimpleDocTemplate, Paragraph, Spacer, Table,
        TableStyle, HRFlowable,
    )
    from reportlab.lib.enums import TA_CENTER, TA_LEFT
    _REPORTLAB = True
except ImportError:
    _REPORTLAB = False

# Anthropic SDK
try:
    import anthropic as _anthropic
    _ANTHROPIC_SDK = True
except ImportError:
    _ANTHROPIC_SDK = False

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
        "agent":   "rankingReportGenerator",
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
DATAFORSEO_BASE = "https://api.dataforseo.com/v3"
GHL_BASE        = "https://services.leadconnectorhq.com"
REGION          = "us-east-1"
CODE_VERSION    = "1.0.0"

# Tags
TAG_REPORT_DELIVERED  = "free-report-delivered"
TAG_OFFER_ELIGIBLE    = "$49-offer-eligible"

# CTR estimates at organic positions
CTR_POSITION = {1: 0.28, 2: 0.15, 3: 0.11, 4: 0.08,
                5: 0.06, 6: 0.05, 7: 0.04, 8: 0.03,
                9: 0.025, 10: 0.02}

# Vertical keyword seeds — city appended at build time
VERTICAL_SEEDS = {
    "HVAC":        ["hvac repair", "ac repair", "heating and cooling", "furnace repair", "air conditioning service"],
    "Plumbing":    ["plumber", "plumbing repair", "drain cleaning", "water heater repair", "emergency plumber"],
    "Roofing":     ["roofer", "roof repair", "roof replacement", "roofing contractor", "roof inspection"],
    "Electrical":  ["electrician", "electrical repair", "electrical contractor", "panel upgrade", "emergency electrician"],
    "Landscaping": ["landscaping", "lawn care", "lawn service", "landscaper", "yard maintenance"],
    "Cleaning":    ["house cleaning", "maid service", "cleaning service", "janitorial service", "commercial cleaning"],
    "Pest":        ["pest control", "exterminator", "bug control", "termite treatment", "rodent control"],
    "Auto":        ["auto repair", "mechanic", "car repair", "oil change", "brake repair"],
    "Dental":      ["dentist", "dental office", "teeth cleaning", "emergency dentist", "family dentist"],
    "Legal":       ["attorney", "lawyer", "law firm", "legal services", "personal injury attorney"],
    "default":     ["local service", "professional service", "service provider", "best service", "top rated service"],
}

# Brand color for PDF
BRAND_BLUE   = colors.HexColor("#1B4FD8")
BRAND_DARK   = colors.HexColor("#0F1E3C")
BRAND_LIGHT  = colors.HexColor("#F0F4FF")
BRAND_RED    = colors.HexColor("#DC2626")
BRAND_GREEN  = colors.HexColor("#16A34A")
BRAND_YELLOW = colors.HexColor("#D97706")


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
# DataForSEO auth header builder
# ----------------------------------------------------------------
def _dfs_headers() -> dict:
    login    = get_ssm("DATAFORSEO_LOGIN", "")
    password = get_ssm("DATAFORSEO_PASSWORD", "")
    creds    = base64.b64encode(f"{login}:{password}".encode()).decode()
    return {
        "Authorization": f"Basic {creds}",
        "Content-Type":  "application/json",
    }


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
# Helpers
# ----------------------------------------------------------------
def _h(contact_id: str) -> str:
    return hashlib.sha256(str(contact_id).encode()).hexdigest()[:16]


def _extract_domain(url: str) -> str:
    if not url:
        return ""
    url = re.sub(r"https?://", "", url.lower().strip())
    url = url.split("/")[0].replace("www.", "")
    return url


def _location_str(city: str, state: str) -> str:
    parts = [p for p in [city, state, "United States"] if p]
    return ",".join(parts)


def _ctr_estimate(position: int, search_volume: int) -> int:
    ctr = CTR_POSITION.get(min(position, 10), 0.01)
    return int(search_volume * ctr)


def _rank_color(rank) -> str:
    if rank is None or rank == "Not Ranked":
        return "red"
    if isinstance(rank, int):
        if rank <= 3:
            return "green"
        if rank <= 10:
            return "yellow"
    return "red"


# ----------------------------------------------------------------
# STEP 1-2 — Build keyword set
# ----------------------------------------------------------------
def build_keyword_set(
    primary_keyword: str,
    city: str,
    vertical: str,
) -> list:
    """Returns list of 5 keywords: primary + 4 related."""
    seeds = VERTICAL_SEEDS.get(vertical, VERTICAL_SEEDS["default"])
    keywords = []

    if primary_keyword:
        kw = primary_keyword.strip().lower()
        if city.lower() not in kw:
            kw = f"{kw} {city.lower()}"
        keywords.append(kw)
    else:
        keywords.append(f"{seeds[0]} {city.lower()}")

    for seed in seeds[1:]:
        candidate = f"{seed} {city.lower()}"
        if candidate not in keywords:
            keywords.append(candidate)
        if len(keywords) == 5:
            break

    while len(keywords) < 5:
        keywords.append(f"best {seeds[0]} {city.lower()}")

    return keywords[:5]


# ----------------------------------------------------------------
# STEP 3 — DataForSEO SERP pull for one keyword
# Per dev guide: use Live endpoint for real-time results
# ----------------------------------------------------------------
def fetch_serp_for_keyword(
    keyword: str,
    location: str,
    contact_id: str,
    prospect_domain: str,
) -> dict:
    """
    Calls DataForSEO Live SERP.
    Returns dict with prospect_rank, search_volume, competitors.
    """
    url  = f"{DATAFORSEO_BASE}/serp/google/organic/live/advanced"
    body = [{
        "keyword":       keyword,
        "location_name": location,
        "language_name": "English",
        "device":        "desktop",
        "os":            "windows",
        "depth":         100,
        "tag":           _h(contact_id),
    }]

    try:
        resp = requests.post(
            url, headers=_dfs_headers(), json=body, timeout=25
        )
        data = resp.json()
        task = data.get("tasks", [{}])[0]

        if task.get("status_code") != 20000:
            log("warning", "dataforseo_task_error",
                keyword=keyword,
                status=task.get("status_code"),
                msg=task.get("status_message"))
            return _empty_serp(keyword)

        result = (task.get("result") or [{}])[0]
        items  = result.get("items") or []

        # Search volume from keyword_data if present
        search_volume = 0
        kw_data = result.get("keyword_data", {})
        if kw_data:
            search_volume = kw_data.get("keyword_info", {}).get("search_volume", 0) or 0

        # Prospect rank from organic items
        prospect_rank = None
        if prospect_domain:
            for item in items:
                if item.get("type") == "organic":
                    item_url = item.get("url", "")
                    if prospect_domain in _extract_domain(item_url):
                        prospect_rank = item.get("rank_absolute")
                        break

        # Top 3 competitors from organic items (excluding prospect)
        competitors = []
        for item in items:
            if item.get("type") != "organic":
                continue
            domain = _extract_domain(item.get("url", ""))
            if prospect_domain and prospect_domain in domain:
                continue
            if domain and domain not in [c["domain"] for c in competitors]:
                competitors.append({
                    "domain": domain,
                    "rank":   item.get("rank_absolute", 99),
                    "title":  item.get("title", domain),
                })
            if len(competitors) == 3:
                break

        log("info", "dataforseo_keyword_fetched",
            keyword=keyword,
            prospect_rank=prospect_rank,
            competitors=len(competitors))

        return {
            "keyword":       keyword,
            "prospect_rank": prospect_rank,
            "search_volume": search_volume,
            "competitors":   competitors,
        }

    except Exception as exc:
        log("error", "dataforseo_exception",
            keyword=keyword, error=str(exc))
        return _empty_serp(keyword)


def _empty_serp(keyword: str) -> dict:
    return {
        "keyword":       keyword,
        "prospect_rank": None,
        "search_volume": 0,
        "competitors":   [],
    }


# ----------------------------------------------------------------
# STEP 4-6 — Parallel SERP fetch + gap score + traffic estimate
# ----------------------------------------------------------------
def fetch_all_keywords_parallel(
    keywords: list,
    location: str,
    contact_id: str,
    prospect_domain: str,
) -> list:
    """Fetches all 5 keywords in parallel threads."""
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as ex:
        futures = {
            ex.submit(
                fetch_serp_for_keyword,
                kw, location, contact_id, prospect_domain
            ): kw
            for kw in keywords
        }
        for future in concurrent.futures.as_completed(futures, timeout=60):
            try:
                results.append(future.result())
            except Exception as exc:
                kw = futures[future]
                log("error", "keyword_future_failed",
                    keyword=kw, error=str(exc))
                results.append(_empty_serp(kw))
    return results


def calculate_gap_score(serp_results: list) -> int:
    """Sum of position gaps between prospect and #1 competitor."""
    total = 0
    for r in serp_results:
        prospect = r.get("prospect_rank")
        comps    = r.get("competitors", [])
        top_rank = comps[0]["rank"] if comps else 1
        if prospect is None:
            total += (100 - top_rank)
        else:
            total += max(0, prospect - top_rank)
    return total


def build_traffic_table(serp_results: list) -> list:
    """Returns list of traffic opportunity rows per keyword."""
    rows = []
    for r in serp_results:
        sv = r.get("search_volume") or 0
        rows.append({
            "keyword":          r["keyword"],
            "search_volume":    sv,
            "traffic_at_pos1":  _ctr_estimate(1, sv),
            "traffic_at_pos3":  _ctr_estimate(3, sv),
            "traffic_at_pos10": _ctr_estimate(10, sv),
        })
    return rows


def get_top_competitors(serp_results: list) -> list:
    """Deduplicated top 3 competitor domains across all keywords."""
    seen   = {}
    for r in serp_results:
        for comp in r.get("competitors", []):
            domain = comp["domain"]
            if domain not in seen:
                seen[domain] = {"domain": domain, "rank": comp["rank"],
                                "title": comp["title"], "kw_count": 0}
            seen[domain]["kw_count"] += 1
    ranked = sorted(seen.values(), key=lambda x: (x["rank"], -x["kw_count"]))
    return ranked[:3]


def get_top_3_gaps(serp_results: list) -> list:
    """Returns top 3 keywords with biggest gaps."""
    gaps = []
    for r in serp_results:
        prospect = r.get("prospect_rank")
        comps    = r.get("competitors", [])
        top_rank = comps[0]["rank"] if comps else 1
        gap      = (100 - top_rank) if prospect is None else max(0, prospect - top_rank)
        gaps.append({"keyword": r["keyword"], "gap": gap,
                     "prospect_rank": prospect,
                     "top_competitor": comps[0]["domain"] if comps else "—"})
    gaps.sort(key=lambda x: x["gap"], reverse=True)
    return gaps[:3]


# ----------------------------------------------------------------
# STEP 7 — Claude narrative generation
# ----------------------------------------------------------------
def generate_narrative(
    business_name: str,
    city: str,
    vertical: str,
    gap_score: int,
    top_gaps: list,
    top_competitors: list,
) -> dict:
    """
    Calls Claude to generate plain-English narrative sections.
    Returns dict with keys: summary, what_this_means, gap_analysis.
    Falls back to template text if Claude unavailable.
    """
    api_key  = get_ssm("ANTHROPIC_API_KEY", "")
    model_id = get_ssm("CLAUDE_MODEL_ID", "claude-sonnet-4-6")

    if not api_key or not _ANTHROPIC_SDK:
        log("warning", "claude_unavailable_using_fallback")
        return _narrative_fallback(business_name, city, gap_score, top_gaps)

    try:
        client = _anthropic.Anthropic(api_key=api_key)

        system_prompt = (
            "You are an SEO analyst writing sections of a free local rankings report "
            "for a small business owner. Your tone is direct, data-driven, and helpful. "
            "Never use jargon. Output JSON only — no prose outside the JSON.\n"
            "Required keys: summary (2 sentences), what_this_means (3 sentences), "
            "gap_analysis (2 sentences per gap, max 3 gaps)."
        )

        gaps_text = "\n".join(
            f"- Keyword: {g['keyword']} | Prospect rank: {g['prospect_rank'] or 'Not Ranked'} "
            f"| Top competitor: {g['top_competitor']} | Gap: {g['gap']}"
            for g in top_gaps
        )
        comps_text = ", ".join(c["domain"] for c in top_competitors) or "unknown"

        user_message = (
            f"Business: {business_name}\n"
            f"City: {city}\n"
            f"Vertical: {vertical}\n"
            f"Ranking Gap Score: {gap_score} (higher = bigger gap from competitors)\n"
            f"Top 3 Keyword Gaps:\n{gaps_text}\n"
            f"Top Competitors: {comps_text}\n\n"
            f"Generate the narrative sections for this report. "
            f"Return JSON only with keys: summary, what_this_means, gap_analysis."
        )

        message = client.messages.create(
            model=model_id,
            max_tokens=1024,
            system=system_prompt,
            messages=[{"role": "user", "content": user_message}],
        )

        raw  = message.content[0].text.strip()
        raw  = re.sub(r"^```json|^```|```$", "", raw, flags=re.MULTILINE).strip()
        data = json.loads(raw)
        log("info", "claude_narrative_generated")
        return data

    except Exception as exc:
        log("warning", "claude_narrative_failed",
            error=str(exc))
        return _narrative_fallback(business_name, city, gap_score, top_gaps)


def _narrative_fallback(
    business_name: str,
    city: str,
    gap_score: int,
    top_gaps: list,
) -> dict:
    top_kw = top_gaps[0]["keyword"] if top_gaps else "your primary keyword"
    return {
        "summary": (
            f"{business_name} is currently not capturing its full share of local "
            f"search traffic in {city}. The data shows clear opportunities to close "
            f"the gap on competitors already ranking above you."
        ),
        "what_this_means": (
            f"When customers search for {top_kw} in {city}, your competitors are "
            f"showing up first — and capturing those calls. Every position you are "
            f"below a competitor is lost revenue. The gap score of {gap_score} "
            f"indicates significant room to improve with the right signals in place."
        ),
        "gap_analysis": (
            f"The biggest opportunity is {top_kw}. Fixing the signals Google uses "
            f"to rank local businesses — GBP health, schema markup, review velocity, "
            f"and content authority — is how businesses move from invisible to Page 1."
        ),
    }


# ----------------------------------------------------------------
# STEP 8 — Build PDF report with reportlab
# ----------------------------------------------------------------
def build_pdf_report(
    business_name: str,
    first_name: str,
    city: str,
    state: str,
    vertical: str,
    serp_results: list,
    top_competitors: list,
    traffic_table: list,
    top_gaps: list,
    gap_score: int,
    narrative: dict,
    checkout_url: str,
) -> bytes:
    """
    Builds a branded 6-section PDF report.
    Returns bytes ready for S3 upload.
    Falls back to minimal PDF if reportlab unavailable.
    """
    if not _REPORTLAB:
        log("warning", "reportlab_missing_minimal_pdf")
        return _minimal_pdf_fallback(business_name, city)

    buf    = io.BytesIO()
    styles = getSampleStyleSheet()
    doc    = SimpleDocTemplate(
        buf,
        pagesize=letter,
        rightMargin=0.75 * inch,
        leftMargin=0.75 * inch,
        topMargin=0.75 * inch,
        bottomMargin=0.75 * inch,
    )

    # Custom styles
    title_style = ParagraphStyle(
        "Title30", parent=styles["Heading1"],
        fontSize=24, textColor=BRAND_DARK,
        spaceAfter=6, alignment=TA_CENTER,
    )
    h2_style = ParagraphStyle(
        "H2_30", parent=styles["Heading2"],
        fontSize=14, textColor=BRAND_BLUE,
        spaceBefore=16, spaceAfter=6,
    )
    body_style = ParagraphStyle(
        "Body30", parent=styles["Normal"],
        fontSize=10, leading=15, spaceAfter=6,
    )
    small_style = ParagraphStyle(
        "Small30", parent=styles["Normal"],
        fontSize=9, textColor=colors.grey,
    )
    cta_style = ParagraphStyle(
        "CTA30", parent=styles["Normal"],
        fontSize=12, textColor=BRAND_BLUE,
        alignment=TA_CENTER, spaceBefore=8,
    )

    story = []

    # ── COVER ────────────────────────────────────────────────
    story.append(Spacer(1, 0.3 * inch))
    story.append(Paragraph("SEO LOCAL", ParagraphStyle(
        "Brand30", parent=styles["Normal"],
        fontSize=11, textColor=BRAND_BLUE,
        alignment=TA_CENTER, spaceAfter=4,
    )))
    story.append(Paragraph(
        f"Free Rankings Report", title_style
    ))
    story.append(Paragraph(
        f"{business_name} — {city}, {state}",
        ParagraphStyle("Sub30", parent=styles["Normal"],
                       fontSize=14, textColor=BRAND_DARK,
                       alignment=TA_CENTER, spaceAfter=4),
    ))
    story.append(Paragraph(
        datetime.now(timezone.utc).strftime("Report generated: %B %d, %Y"),
        ParagraphStyle("Date30", parent=styles["Normal"],
                       fontSize=9, textColor=colors.grey,
                       alignment=TA_CENTER, spaceAfter=8),
    ))

    # Gap score badge
    score_label = "NEEDS ATTENTION" if gap_score > 150 else ("IMPROVING" if gap_score > 50 else "STRONG")
    score_color = BRAND_RED if gap_score > 150 else (BRAND_YELLOW if gap_score > 50 else BRAND_GREEN)
    score_data  = [[
        Paragraph(f"Ranking Gap Score: {gap_score}", ParagraphStyle(
            "Score30", parent=styles["Normal"],
            fontSize=16, textColor=colors.white,
            alignment=TA_CENTER,
        )),
        Paragraph(score_label, ParagraphStyle(
            "ScoreLbl30", parent=styles["Normal"],
            fontSize=11, textColor=colors.white,
            alignment=TA_CENTER,
        )),
    ]]
    score_table = Table(score_data, colWidths=[3.5 * inch, 3.5 * inch])
    score_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (0, 0), BRAND_DARK),
        ("BACKGROUND", (1, 0), (1, 0), score_color),
        ("ALIGN",      (0, 0), (-1, -1), "CENTER"),
        ("VALIGN",     (0, 0), (-1, -1), "MIDDLE"),
        ("ROWBACKGROUNDS", (0, 0), (-1, -1), [None]),
        ("TOPPADDING",    (0, 0), (-1, -1), 10),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 10),
    ]))
    story.append(score_table)
    story.append(HRFlowable(width="100%", thickness=1,
                             color=BRAND_BLUE, spaceAfter=12))

    # ── SECTION 1: Your Current Rankings ─────────────────────
    story.append(Paragraph("Section 1 — Your Current Rankings", h2_style))

    rank_data = [["Keyword", "Your Position", "Search Volume / Mo"]]
    for r in serp_results:
        pos = r["prospect_rank"] if r["prospect_rank"] else "Not Ranked"
        rank_data.append([
            r["keyword"].title(),
            str(pos),
            f"{r.get('search_volume', 0):,}",
        ])

    rank_table = Table(rank_data, colWidths=[3.5 * inch, 2 * inch, 1.5 * inch])
    rank_table.setStyle(TableStyle([
        ("BACKGROUND",   (0, 0), (-1, 0), BRAND_BLUE),
        ("TEXTCOLOR",    (0, 0), (-1, 0), colors.white),
        ("FONTNAME",     (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE",     (0, 0), (-1, 0), 9),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [BRAND_LIGHT, colors.white]),
        ("FONTSIZE",     (0, 1), (-1, -1), 9),
        ("ALIGN",        (1, 0), (-1, -1), "CENTER"),
        ("GRID",         (0, 0), (-1, -1), 0.3, colors.lightgrey),
        ("TOPPADDING",   (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING",(0, 0), (-1, -1), 5),
    ]))
    story.append(rank_table)
    story.append(Spacer(1, 0.1 * inch))

    # ── SECTION 2: Competitor Comparison ─────────────────────
    story.append(Paragraph("Section 2 — Competitor Comparison", h2_style))
    story.append(Paragraph(
        f"Top competitors ranking for the same keywords in {city}:",
        body_style
    ))

    comp_data = [["Competitor", "Ranking For (keywords)", "Avg Position"]]
    for c in top_competitors:
        comps_ranking = [
            r["keyword"].title()
            for r in serp_results
            if any(comp["domain"] == c["domain"]
                   for comp in r.get("competitors", []))
        ]
        comp_data.append([
            c["domain"],
            ", ".join(comps_ranking[:2]) or "multiple keywords",
            str(c["rank"]),
        ])

    if len(comp_data) == 1:
        comp_data.append(["No competitors identified", "—", "—"])

    comp_table = Table(comp_data, colWidths=[2 * inch, 3.2 * inch, 1.8 * inch])
    comp_table.setStyle(TableStyle([
        ("BACKGROUND",   (0, 0), (-1, 0), BRAND_DARK),
        ("TEXTCOLOR",    (0, 0), (-1, 0), colors.white),
        ("FONTNAME",     (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE",     (0, 0), (-1, 0), 9),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [BRAND_LIGHT, colors.white]),
        ("FONTSIZE",     (0, 1), (-1, -1), 9),
        ("GRID",         (0, 0), (-1, -1), 0.3, colors.lightgrey),
        ("TOPPADDING",   (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING",(0, 0), (-1, -1), 5),
    ]))
    story.append(comp_table)
    story.append(Spacer(1, 0.1 * inch))

    # ── SECTION 3: Traffic Opportunity ───────────────────────
    story.append(Paragraph("Section 3 — Traffic Opportunity", h2_style))
    story.append(Paragraph(
        "Estimated monthly visitors if you ranked at each position:",
        body_style
    ))

    traffic_data = [["Keyword", "Vol/Mo", "At #1", "At #3", "At #10"]]
    total_opp = 0
    for row in traffic_table:
        traffic_data.append([
            row["keyword"].title()[:35],
            f"{row['search_volume']:,}",
            f"{row['traffic_at_pos1']:,}",
            f"{row['traffic_at_pos3']:,}",
            f"{row['traffic_at_pos10']:,}",
        ])
        total_opp += row["traffic_at_pos1"]

    traf_table = Table(
        traffic_data,
        colWidths=[2.8 * inch, 0.9 * inch, 0.9 * inch, 0.9 * inch, 0.9 * inch]
    )
    traf_table.setStyle(TableStyle([
        ("BACKGROUND",   (0, 0), (-1, 0), BRAND_BLUE),
        ("TEXTCOLOR",    (0, 0), (-1, 0), colors.white),
        ("FONTNAME",     (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE",     (0, 0), (-1, 0), 9),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [BRAND_LIGHT, colors.white]),
        ("FONTSIZE",     (0, 1), (-1, -1), 9),
        ("ALIGN",        (1, 0), (-1, -1), "CENTER"),
        ("GRID",         (0, 0), (-1, -1), 0.3, colors.lightgrey),
        ("TOPPADDING",   (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING",(0, 0), (-1, -1), 5),
    ]))
    story.append(traf_table)
    story.append(Paragraph(
        f"Total traffic opportunity at full Page 1 coverage: "
        f"<b>{total_opp:,} visitors/month</b>",
        ParagraphStyle("Opp30", parent=body_style,
                       textColor=BRAND_GREEN, spaceBefore=6),
    ))

    # ── SECTION 4: Your 3 Biggest Gaps ───────────────────────
    story.append(Paragraph("Section 4 — Your 3 Biggest Gaps", h2_style))
    for i, gap in enumerate(top_gaps, 1):
        rank_str = (f"#{gap['prospect_rank']}" if gap["prospect_rank"]
                    else "Not Ranked")
        story.append(Paragraph(
            f"<b>Gap {i}: {gap['keyword'].title()}</b>",
            body_style,
        ))
        story.append(Paragraph(
            f"Your position: {rank_str} | "
            f"Top competitor: {gap['top_competitor']} | "
            f"Gap size: {gap['gap']} positions",
            small_style,
        ))
        story.append(Spacer(1, 0.05 * inch))

    gap_text = narrative.get("gap_analysis", "")
    if gap_text:
        story.append(Paragraph(gap_text, body_style))

    # ── SECTION 5: What This Means ───────────────────────────
    story.append(Paragraph("Section 5 — What This Means", h2_style))
    story.append(Paragraph(
        narrative.get("summary", ""), body_style
    ))
    story.append(Paragraph(
        narrative.get("what_this_means", ""), body_style
    ))

    # ── SECTION 6: CTA ───────────────────────────────────────
    story.append(HRFlowable(width="100%", thickness=1,
                             color=BRAND_BLUE, spaceBefore=12))
    story.append(Paragraph("Ready to Close These Gaps?", h2_style))
    story.append(Paragraph(
        f"Get the full $49 Deep Dive Audit — a complete 30-point analysis "
        f"of {business_name}'s local SEO signals, with a prioritised fix list "
        f"ranked by how fast each one moves your rankings.",
        body_style,
    ))
    if checkout_url:
        story.append(Paragraph(
            f'<a href="{checkout_url}" color="#1B4FD8">'
            f"Get Your Full Deep Dive Audit ($49) →</a>",
            cta_style,
        ))
    story.append(Spacer(1, 0.2 * inch))
    story.append(Paragraph(
        "SEO Local | chuck@seolocal.us | seolocal.us",
        ParagraphStyle("Footer30", parent=styles["Normal"],
                       fontSize=8, textColor=colors.grey,
                       alignment=TA_CENTER),
    ))

    doc.build(story)
    return buf.getvalue()


def _minimal_pdf_fallback(business_name: str, city: str) -> bytes:
    """Returns a minimal valid PDF when reportlab is unavailable."""
    content = (
        f"%PDF-1.4\n"
        f"1 0 obj<</Type/Catalog/Pages 2 0 R>>endobj\n"
        f"2 0 obj<</Type/Pages/Kids[3 0 R]/Count 1>>endobj\n"
        f"3 0 obj<</Type/Page/MediaBox[0 0 612 792]/Parent 2 0 R"
        f"/Contents 4 0 R/Resources<</Font<</F1<</Type/Font"
        f"/Subtype/Type1/BaseFont/Helvetica>>>>>>>>endobj\n"
        f"4 0 obj<</Length 80>>\nstream\n"
        f"BT /F1 16 Tf 72 700 Td "
        f"({business_name} Rankings Report - {city}) Tj ET\n"
        f"endstream\nendobj\n"
        f"xref\n0 5\n0000000000 65535 f\n"
        f"trailer<</Size 5/Root 1 0 R>>\n%%EOF"
    )
    return content.encode()


# ----------------------------------------------------------------
# STEP 9 — Upload PDF to S3
# ----------------------------------------------------------------
def upload_to_s3(
    pdf_bytes: bytes,
    contact_id: str,
    bucket: str,
) -> str:
    """Uploads PDF to S3 and returns the public/presigned URL."""
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    s3_key   = f"rankings-reports/{contact_id}-{date_str}.pdf"

    try:
        s3 = boto3.client("s3", region_name=REGION)
        s3.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=pdf_bytes,
            ContentType="application/pdf",
        )

        # Generate presigned URL valid for 30 days
        url = s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket, "Key": s3_key},
            ExpiresIn=30 * 24 * 3600,
        )
        log("info", "s3_upload_success",
            contact_id_hash=_h(contact_id), key=s3_key)
        return url

    except Exception as exc:
        log("error", "s3_upload_failed",
            contact_id_hash=_h(contact_id), error=str(exc))
        return ""


# ----------------------------------------------------------------
# STEP 10 — GHL email delivery
# ----------------------------------------------------------------
def _html_email(plain: str) -> str:
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


def ghl_send_delivery_email(
    contact_id: str,
    first_name: str,
    city: str,
    business_name: str,
    report_url: str,
    checkout_url: str,
) -> bool:
    subject = f"Your {city} Rankings Report is Ready, {first_name}"
    body = (
        f"Hey {first_name},\n\n"
        f"Your free local rankings report for {business_name} is ready.\n\n"
        f"View your report here:\n"
        f"{report_url}\n\n"
        f"What your report shows:\n"
        f"  → Where {business_name} currently ranks for your top keywords in {city}\n"
        f"  → Which competitors are outranking you and by how much\n"
        f"  → Estimated monthly traffic you're leaving on the table\n"
        f"  → Your 3 biggest ranking gaps and what's causing them\n\n"
        f"The report data is live as of today. Rankings shift — "
        f"the sooner you move on the gaps, the better the head start.\n\n"
        f"Want the full picture? The $49 Deep Dive Audit goes 30 points "
        f"deeper — with a prioritised fix list and a clear path to Page 1:\n"
        f"{checkout_url}\n\n"
        f"— Chuck Gray\n"
        f"SEO Local\n"
        f"chuck@seolocal.us"
    )

    resp = _ghl(
        "POST",
        f"{GHL_BASE}/conversations/messages",
        json={
            "type":    "Email",
            "subject": subject,
            "html":    _html_email(body),
            "to":      [{"type": "contact", "contactId": contact_id}],
        },
    )
    ok = resp.status_code in (200, 201)
    if not ok:
        log("warning", "ghl_email_failed",
            contact_id_hash=_h(contact_id), status=resp.status_code)
    return ok


# ----------------------------------------------------------------
# STEP 11-13 — GHL fields, tags, stage
# ----------------------------------------------------------------
def ghl_update_fields(contact_id: str, fields: list) -> bool:
    resp = _ghl(
        "PUT",
        f"{GHL_BASE}/contacts/{contact_id}",
        json={"customFields": fields},
    )
    return resp.status_code in (200, 201)


def ghl_add_tags(contact_id: str, tags: list) -> bool:
    resp = _ghl(
        "POST",
        f"{GHL_BASE}/contacts/{contact_id}/tags",
        json={"tags": tags},
    )
    return resp.status_code in (200, 201)


def ghl_move_stage(contact_id: str) -> bool:
    pipeline_id = get_ssm("GHL_PIPELINE_ID", "xEK3qOXCRezO6aoHN6AS")
    stage_id    = get_ssm("GHL_STAGE_PAID_REPORT_DELIVERED", "")
    if not stage_id:
        log("warning", "ghl_stage_missing",
            note="GHL_STAGE_PAID_REPORT_DELIVERED not in SSM")
        return False

    # Find or create opportunity in pipeline
    search = _ghl(
        "GET",
        f"{GHL_BASE}/opportunities/search",
        params={"contact_id": contact_id, "pipeline_id": pipeline_id},
    )
    opps = search.json().get("opportunities", []) if search.status_code == 200 else []

    if opps:
        opp_id = opps[0]["id"]
        resp   = _ghl(
            "PUT",
            f"{GHL_BASE}/opportunities/{opp_id}",
            json={"pipelineId": pipeline_id, "pipelineStageId": stage_id},
        )
    else:
        resp = _ghl(
            "POST",
            f"{GHL_BASE}/opportunities/",
            json={
                "pipelineId":      pipeline_id,
                "pipelineStageId": stage_id,
                "contactId":       contact_id,
                "name":            "Free Rankings Report Delivered",
                "status":          "open",
            },
        )

    ok = resp.status_code in (200, 201)
    if not ok:
        log("warning", "ghl_stage_move_failed",
            contact_id_hash=_h(contact_id), status=resp.status_code)
    return ok


# ----------------------------------------------------------------
# CORE AGENT LOGIC
# ----------------------------------------------------------------
def run_ranking_report_generator(payload: dict) -> dict:
    now = datetime.now(timezone.utc)

    # --- Validate required fields ---
    contact_id    = (payload.get("contact_id") or "").strip()
    first_name    = (payload.get("first_name") or "there").strip()
    email         = (payload.get("email") or "").strip()
    business_name = (payload.get("business_name") or "").strip()
    city          = (payload.get("city") or "").strip()
    state         = (payload.get("state") or "").strip()
    vertical      = (payload.get("vertical") or "default").strip()
    website_url   = (payload.get("website_url") or "").strip()
    primary_kw    = (payload.get("primary_keyword") or "").strip()

    if not contact_id:
        return {"status": "error", "code": "MISSING_CONTACT_ID"}
    if not business_name:
        return {"status": "error", "code": "MISSING_BUSINESS_NAME"}
    if not city:
        return {"status": "error", "code": "MISSING_CITY"}

    s3_bucket    = get_ssm("S3_REPORTS_BUCKET", "")
    checkout_url = get_ssm(
        "RECURLY_DEEP_DIVE_CHECKOUT_URL",
        "https://seolocal.us/deep-dive"
    )
    prospect_domain = _extract_domain(website_url)
    location        = _location_str(city, state)

    log("info", "report_start",
        contact_id_hash=_h(contact_id),
        business_name=business_name,
        city=city, vertical=vertical)

    # --- Step 1-2: Build keyword set ---
    keywords = build_keyword_set(primary_kw, city, vertical)
    log("info", "keywords_built", keywords=keywords)

    # --- Step 3-4: Parallel DataForSEO SERP fetch ---
    serp_results = fetch_all_keywords_parallel(
        keywords, location, contact_id, prospect_domain
    )

    # --- Step 5-6: Gap score + traffic opportunity ---
    gap_score       = calculate_gap_score(serp_results)
    traffic_table   = build_traffic_table(serp_results)
    top_competitors = get_top_competitors(serp_results)
    top_gaps        = get_top_3_gaps(serp_results)

    log("info", "analysis_complete",
        contact_id_hash=_h(contact_id),
        gap_score=gap_score,
        competitors_found=len(top_competitors))

    # --- Step 7: Claude narrative ---
    narrative = generate_narrative(
        business_name, city, vertical,
        gap_score, top_gaps, top_competitors
    )

    # --- Step 8: Build PDF ---
    pdf_bytes = build_pdf_report(
        business_name=business_name,
        first_name=first_name,
        city=city,
        state=state,
        vertical=vertical,
        serp_results=serp_results,
        top_competitors=top_competitors,
        traffic_table=traffic_table,
        top_gaps=top_gaps,
        gap_score=gap_score,
        narrative=narrative,
        checkout_url=checkout_url,
    )
    log("info", "pdf_built",
        contact_id_hash=_h(contact_id),
        size_bytes=len(pdf_bytes))

    # --- Step 9: Upload to S3 ---
    report_url = ""
    if s3_bucket:
        report_url = upload_to_s3(pdf_bytes, contact_id, s3_bucket)
    else:
        log("warning", "s3_bucket_missing",
            note="S3_REPORTS_BUCKET not in SSM — skipping upload")

    # --- Step 10: GHL email delivery ---
    email_ok = ghl_send_delivery_email(
        contact_id, first_name, city,
        business_name, report_url or checkout_url, checkout_url
    )

    # --- Step 11: Update GHL custom fields ---
    ghl_update_fields(contact_id, [
        {"key": "DELIVERABLE_URL",            "field_value": report_url},
        {"key": "RANKING_REPORT_DELIVERED",   "field_value": "true"},
        {"key": "ranking_gap_score",          "field_value": str(gap_score)},
        {"key": "ranking_report_date",        "field_value": now.isoformat()},
        {"key": "primary_keyword",
         "field_value": keywords[0] if keywords else ""},
        {"key": "current_rank_primary",
         "field_value": str(
             serp_results[0]["prospect_rank"] if serp_results and
             serp_results[0]["prospect_rank"] else "Not Ranked"
         )},
    ])

    # --- Step 12: Apply GHL tags ---
    ghl_add_tags(contact_id, [TAG_REPORT_DELIVERED, TAG_OFFER_ELIGIBLE])

    # --- Step 13: Move GHL stage ---
    stage_ok = ghl_move_stage(contact_id)

    log("info", "report_complete",
        contact_id_hash=_h(contact_id),
        gap_score=gap_score,
        email_ok=email_ok,
        report_url_generated=bool(report_url),
        stage_moved=stage_ok,
        version=CODE_VERSION)

    return {
        "status":            "delivered",
        "contact_id":        contact_id,
        "business_name":     business_name,
        "city":              city,
        "keywords_checked":  len(serp_results),
        "gap_score":         gap_score,
        "competitors_found": len(top_competitors),
        "report_url":        report_url,
        "email_sent":        email_ok,
        "stage_moved":       stage_ok,
        "agent":             "rankingReportGenerator",
        "version":           CODE_VERSION,
    }


# ----------------------------------------------------------------
# AgentCore entrypoint
# Rules:
#   1. payload: dict only — no context param (causes HTTP 400)
#   2. app.run() is mandatory — agent times out without it
#   3. SystemExit and BaseException caught separately
# ----------------------------------------------------------------
if _SDK:
    app = BedrockAgentCoreApp()

    @app.entrypoint
    def ranking_report_generator(payload: dict) -> dict:
        try:
            return run_ranking_report_generator(payload)
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
