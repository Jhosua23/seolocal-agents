"""
================================================================
Agent 14 — Heat Map Generator
================================================================
AgentCore name  : heatMapGenerator
Phase           : 1 - Lead Generation
Sequence        : 14 of 15
Priority        : P1
Runtime         : Python 3.12 / ARM64
Version         : 1.0.0

Generates geo-grid Map Pack heat map using Local Falcon API.
Uploads PNG + PDF to S3. Updates GHL contact on purchase.
================================================================

SSM Parameters required:
  LOCAL_FALCON_API_KEY       - SecureString  (ask Chuck)
  GOOGLE_PLACES_API_KEY      - SecureString  (already in SSM)
  GHL_API_KEY                - SecureString  (already in SSM)
  GHL_PIPELINE_ID            - String        (already in SSM)
  GHL_STAGE_HEAT_MAP_DELIV   - String        (ask Chuck for stage ID)
  S3_REPORTS_BUCKET          - String        (ask Chuck)
  GHL_DEFAULT_ASSIGNEE_ID    - String        (already in SSM)
  RECURLY_WEBHOOK_SECRET     - SecureString  (ask Chuck)

Trigger  : POST /generate/heat-map
           trigger_source: purchase | deep_dive_agent | manual
================================================================
"""

import hashlib
import io
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
        "agent":   "heatMapGenerator",
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
GHL_BASE       = "https://services.leadconnectorhq.com"
LOCAL_FALCON_BASE = "https://localfalcon.com/api/v2"
GEOCODING_BASE = "https://maps.googleapis.com/maps/api/geocode/json"
REGION         = "us-east-1"
CODE_VERSION   = "1.0.0"

VALID_GRID_SIZES = {"5x5", "7x7", "13x13"}
DEFAULT_GRID     = "7x7"
SCAN_POLL_INTERVAL = 10   # seconds between status polls
SCAN_MAX_WAIT      = 180  # seconds max wait for scan


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

    raise RuntimeError(f"GHL failed: {url}")


# ----------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------
def _h(contact_id: str) -> str:
    """Hash contact_id — never log PII to CloudWatch."""
    return hashlib.sha256(str(contact_id).encode()).hexdigest()[:16]


# ----------------------------------------------------------------
# GHL — Update contact
# ----------------------------------------------------------------
def ghl_update_contact(contact_id: str, fields: list, tags: list) -> bool:
    ok1 = True
    if fields:
        resp = _ghl(
            "PUT",
            f"{GHL_BASE}/contacts/{contact_id}",
            json={"customFields": fields},
        )
        ok1 = resp.status_code in (200, 201)

    ok2 = True
    if tags:
        resp = _ghl(
            "POST",
            f"{GHL_BASE}/contacts/{contact_id}/tags",
            json={"tags": tags},
        )
        ok2 = resp.status_code in (200, 201)

    return ok1 and ok2


# ----------------------------------------------------------------
# GHL — Move pipeline stage
# ----------------------------------------------------------------
def ghl_move_stage(contact_id: str, business_name: str) -> bool:
    pipeline_id = get_ssm("GHL_PIPELINE_ID")
    stage_id    = get_ssm("GHL_STAGE_HEAT_MAP_DELIV", "")
    if not stage_id:
        log("warning", "ghl_stage_id_missing",
            note="GHL_STAGE_HEAT_MAP_DELIV not in SSM")
        return False

    search = _ghl(
        "GET",
        f"{GHL_BASE}/opportunities/search",
        params={"contact_id": contact_id, "pipeline_id": pipeline_id},
    )
    opps = search.json().get("opportunities", []) if search.status_code == 200 else []

    if opps:
        opp_id = opps[0]["id"]
    else:
        resp = _ghl(
            "POST",
            f"{GHL_BASE}/opportunities/",
            json={
                "pipelineId":      pipeline_id,
                "pipelineStageId": stage_id,
                "contactId":       contact_id,
                "name":            f"{business_name} — SEO Local",
                "status":          "open",
                "monetaryValue":   39,
            },
        )
        if resp.status_code not in (200, 201):
            return False
        opp_id = resp.json().get("opportunity", {}).get("id", "")

    if opp_id:
        resp = _ghl(
            "PUT",
            f"{GHL_BASE}/opportunities/{opp_id}",
            json={
                "pipelineId":      pipeline_id,
                "pipelineStageId": stage_id,
                "status":          "open",
            },
        )
        return resp.status_code in (200, 201)
    return False


# ----------------------------------------------------------------
# GHL — Create urgent task
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
            "dueDate":    (datetime.now(timezone.utc) + timedelta(hours=2)).isoformat() + "Z",
            "completed":  False,
        },
    )
    return resp.status_code in (200, 201)


# ----------------------------------------------------------------
# Google Geocoding — address to lat/lng (REST, not gRPC)
# ----------------------------------------------------------------
def geocode_address(address: str) -> tuple:
    """Returns (lat, lng) or raises on failure."""
    api_key = get_ssm("GOOGLE_PLACES_API_KEY")
    resp = requests.get(
        GEOCODING_BASE,
        params={"address": address, "key": api_key},
        timeout=10,
    )
    results = resp.json().get("results", [])
    if not results:
        raise ValueError(f"Geocoding failed for address: {address}")
    loc = results[0]["geometry"]["location"]
    return loc["lat"], loc["lng"]


# ----------------------------------------------------------------
# Local Falcon — Scan
# ----------------------------------------------------------------
def start_local_falcon_scan(
    business_name: str,
    keyword: str,
    lat: float,
    lng: float,
    grid_size: str,
) -> str:
    """Starts a scan and returns scan_id."""
    api_key = get_ssm("LOCAL_FALCON_API_KEY")
    resp = requests.post(
        f"{LOCAL_FALCON_BASE}/scan",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type":  "application/json",
        },
        json={
            "business_name": business_name,
            "keyword":       keyword,
            "latitude":      lat,
            "longitude":     lng,
            "grid_size":     grid_size,
            "distance":      1.0,
        },
        timeout=30,
    )
    if resp.status_code not in (200, 201, 202):
        raise RuntimeError(
            f"Local Falcon scan start failed: {resp.status_code} {resp.text[:200]}"
        )
    return resp.json().get("scan_id") or resp.json().get("id", "")


def poll_local_falcon_scan(scan_id: str) -> dict:
    """Polls until complete or timeout. Returns results dict."""
    api_key  = get_ssm("LOCAL_FALCON_API_KEY")
    headers  = {"Authorization": f"Bearer {api_key}"}
    elapsed  = 0

    while elapsed < SCAN_MAX_WAIT:
        resp = requests.get(
            f"{LOCAL_FALCON_BASE}/scan/{scan_id}",
            headers=headers,
            timeout=15,
        )
        data   = resp.json()
        status = data.get("status", "").lower()

        if status == "complete":
            return data
        if status in ("failed", "error"):
            raise RuntimeError(f"Local Falcon scan failed: {data}")

        time.sleep(SCAN_POLL_INTERVAL)
        elapsed += SCAN_POLL_INTERVAL

    raise TimeoutError(f"Local Falcon scan timed out after {SCAN_MAX_WAIT}s")


def parse_scan_results(scan_data: dict, grid_size: str) -> dict:
    """Extract position matrix and summary stats from scan results."""
    grid_points = scan_data.get("grid_points") or scan_data.get("results", [])
    positions   = []

    for point in grid_points:
        pos = point.get("position") or point.get("rank", 20)
        positions.append({
            "lat":      point.get("lat") or point.get("latitude", 0),
            "lng":      point.get("lng") or point.get("longitude", 0),
            "position": int(pos) if str(pos).isdigit() else 21,
        })

    total = len(positions)
    if total == 0:
        return {"positions": [], "avg_position": None,
                "pct_in_pack": 0, "pct_top1": 0, "coverage_radius_miles": 0}

    in_pack   = [p for p in positions if p["position"] <= 3]
    top1      = [p for p in positions if p["position"] == 1]
    ranked    = [p for p in positions if p["position"] <= 20]
    avg_pos   = round(sum(p["position"] for p in ranked) / len(ranked), 1) if ranked else None
    pct_pack  = round((len(in_pack) / total) * 100, 1)
    pct_top1  = round((len(top1)    / total) * 100, 1)
    # Approximate coverage radius based on % in pack
    coverage  = round((pct_pack / 100) * (int(grid_size.split("x")[0]) / 2), 1)

    return {
        "positions":             positions,
        "avg_position":          avg_pos,
        "pct_in_pack":           pct_pack,
        "pct_top1":              pct_top1,
        "coverage_radius_miles": coverage,
    }


# ----------------------------------------------------------------
# Heat map PNG generator (matplotlib)
# ----------------------------------------------------------------
def generate_heatmap_png(positions: list, grid_size: str,
                          business_name: str, keyword: str) -> bytes:
    """
    Generates a colour-coded grid PNG.
    Returns PNG bytes.
    Colour scale: green=1, yellow=2-3, orange=4-10, red=11-20, gray=not ranked
    """
    try:
        import matplotlib
        matplotlib.use("Agg")   # non-interactive backend — required in Lambda/AgentCore
        import matplotlib.pyplot as plt
        import matplotlib.patches as mpatches
        import numpy as np

        n = int(grid_size.split("x")[0])
        grid = np.zeros((n, n))

        for i, point in enumerate(positions):
            row = i // n
            col = i % n
            if row < n and col < n:
                grid[row][col] = point.get("position", 21)

        def pos_to_color(pos):
            if pos == 1:
                return "#22c55e"   # green
            if pos <= 3:
                return "#facc15"   # yellow
            if pos <= 10:
                return "#f97316"   # orange
            if pos <= 20:
                return "#ef4444"   # red
            return "#9ca3af"       # gray — not ranked

        fig, ax = plt.subplots(figsize=(8, 8))
        fig.patch.set_facecolor("#1e293b")
        ax.set_facecolor("#1e293b")

        for row in range(n):
            for col in range(n):
                pos = int(grid[row][col])
                color = pos_to_color(pos)
                rect = mpatches.FancyBboxPatch(
                    (col, n - row - 1), 0.9, 0.9,
                    boxstyle="round,pad=0.05",
                    facecolor=color, edgecolor="#1e293b", linewidth=1.5,
                )
                ax.add_patch(rect)
                label = str(pos) if pos <= 20 else "—"
                ax.text(col + 0.45, n - row - 1 + 0.45, label,
                        ha="center", va="center",
                        fontsize=11, fontweight="bold", color="white")

        ax.set_xlim(0, n)
        ax.set_ylim(0, n)
        ax.axis("off")

        # Legend
        legend_items = [
            mpatches.Patch(color="#22c55e", label="#1"),
            mpatches.Patch(color="#facc15", label="#2–3"),
            mpatches.Patch(color="#f97316", label="#4–10"),
            mpatches.Patch(color="#ef4444", label="#11–20"),
            mpatches.Patch(color="#9ca3af", label="Not ranked"),
        ]
        ax.legend(handles=legend_items, loc="lower center",
                  bbox_to_anchor=(0.5, -0.06), ncol=5,
                  frameon=False, labelcolor="white", fontsize=9)

        ax.set_title(
            f"{business_name}\n{keyword}",
            color="white", fontsize=13, fontweight="bold", pad=12,
        )

        buf = io.BytesIO()
        plt.savefig(buf, format="png", dpi=150,
                    bbox_inches="tight", facecolor=fig.get_facecolor())
        plt.close(fig)
        buf.seek(0)
        return buf.read()

    except ImportError:
        log("warning", "matplotlib_not_available",
            note="matplotlib not installed — returning empty PNG placeholder")
        return b""


# ----------------------------------------------------------------
# S3 — Upload and generate signed URL
# ----------------------------------------------------------------
def s3_upload(data: bytes, key: str, content_type: str) -> str:
    """Uploads bytes to S3, returns signed URL (72hr expiry)."""
    bucket = get_ssm("S3_REPORTS_BUCKET")
    if not bucket:
        log("warning", "s3_bucket_missing", note="S3_REPORTS_BUCKET not in SSM")
        return ""
    s3 = boto3.client("s3", region_name=REGION)
    s3.put_object(Bucket=bucket, Key=key, Body=data, ContentType=content_type)
    url = s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": bucket, "Key": key},
        ExpiresIn=259200,   # 72 hours
    )
    return url


def s3_key_png(contact_id: str, keyword: str, date_str: str) -> str:
    slug = keyword.lower().replace(" ", "-")[:40]
    return f"heat-maps/{contact_id}-{slug}-{date_str}.png"


def s3_key_pdf(contact_id: str, keyword: str, date_str: str) -> str:
    slug = keyword.lower().replace(" ", "-")[:40]
    return f"heat-maps/{contact_id}-{slug}-{date_str}.pdf"


# ----------------------------------------------------------------
# PDF report builder (reportlab)
# ----------------------------------------------------------------
def generate_pdf_report(
    business_name: str,
    keyword: str,
    stats: dict,
    png_bytes: bytes,
    png_url: str,
) -> bytes:
    try:
        from reportlab.lib.pagesizes import letter
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.lib.units import inch
        from reportlab.platypus import (
            SimpleDocTemplate, Paragraph, Spacer, Image, Table, TableStyle,
        )
        from reportlab.lib import colors

        buf = io.BytesIO()
        doc = SimpleDocTemplate(buf, pagesize=letter,
                                leftMargin=inch, rightMargin=inch,
                                topMargin=inch, bottomMargin=inch)
        styles = getSampleStyleSheet()
        story  = []

        # Title
        title_style = ParagraphStyle(
            "Title", parent=styles["Title"],
            fontSize=20, spaceAfter=6, textColor=colors.HexColor("#1e293b"),
        )
        story.append(Paragraph("Local Map Pack Heat Map Report", title_style))
        story.append(Paragraph(f"<b>{business_name}</b>", styles["Heading2"]))
        story.append(Paragraph(f"Keyword: {keyword}", styles["Normal"]))
        story.append(Spacer(1, 0.2 * inch))

        # Stats table
        table_data = [
            ["Metric", "Value"],
            ["Average Position",   str(stats.get("avg_position", "N/A"))],
            ["% In Map Pack (1-3)", f"{stats.get('pct_in_pack', 0)}%"],
            ["% Ranking #1",        f"{stats.get('pct_top1', 0)}%"],
            ["Coverage Radius",     f"{stats.get('coverage_radius_miles', 0)} miles"],
        ]
        t = Table(table_data, colWidths=[3 * inch, 3 * inch])
        t.setStyle(TableStyle([
            ("BACKGROUND",  (0, 0), (-1, 0),  colors.HexColor("#1e293b")),
            ("TEXTCOLOR",   (0, 0), (-1, 0),  colors.white),
            ("FONTNAME",    (0, 0), (-1, 0),  "Helvetica-Bold"),
            ("FONTSIZE",    (0, 0), (-1, -1), 11),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1),
             [colors.HexColor("#f8fafc"), colors.white]),
            ("GRID",        (0, 0), (-1, -1), 0.5, colors.HexColor("#e2e8f0")),
            ("TOPPADDING",  (0, 0), (-1, -1), 6),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
        ]))
        story.append(t)
        story.append(Spacer(1, 0.3 * inch))

        # Heat map image
        if png_bytes:
            img_buf = io.BytesIO(png_bytes)
            img = Image(img_buf, width=5 * inch, height=5 * inch)
            story.append(img)
        story.append(Spacer(1, 0.2 * inch))

        # CTA
        story.append(Paragraph(
            "<b>Ready to dominate your local Map Pack?</b> "
            "Book a free strategy call with our team to see exactly "
            "what it takes to get you ranking #1 in your area.",
            styles["Normal"],
        ))
        story.append(Paragraph(
            f"Report generated: {datetime.now(timezone.utc).strftime('%B %d, %Y')} "
            f"| SEO Local",
            ParagraphStyle("Footer", parent=styles["Normal"],
                           fontSize=8, textColor=colors.grey),
        ))

        doc.build(story)
        buf.seek(0)
        return buf.read()

    except ImportError:
        log("warning", "reportlab_not_available",
            note="reportlab not installed — skipping PDF generation")
        return b""


# ----------------------------------------------------------------
# CORE AGENT LOGIC
# ----------------------------------------------------------------
def run_heat_map_generator(payload: dict) -> dict:
    now      = datetime.now(timezone.utc)
    date_str = now.strftime("%Y%m%d")

    # --- Validate inputs ---
    business_name  = (payload.get("business_name") or "").strip()
    address        = (payload.get("address") or "").strip()
    keyword        = (payload.get("keyword") or "").strip()
    grid_size      = payload.get("grid_size", DEFAULT_GRID)
    contact_id     = (payload.get("contact_id") or "").strip()
    trigger_source = (payload.get("trigger_source") or "manual").strip()
    email          = (payload.get("email") or "").strip()

    if not business_name:
        return {"status": "error", "code": "MISSING_BUSINESS_NAME"}
    if not address:
        return {"status": "error", "code": "MISSING_ADDRESS"}
    if not keyword:
        return {"status": "error", "code": "MISSING_KEYWORD"}
    if grid_size not in VALID_GRID_SIZES:
        grid_size = DEFAULT_GRID

    log("info", "heat_map_start",
        contact_id_hash=_h(contact_id) if contact_id else "none",
        keyword=keyword, grid_size=grid_size, trigger=trigger_source)

    # --- Step 1: Geocode address ---
    try:
        lat, lng = geocode_address(address)
        log("info", "geocoded", lat=lat, lng=lng)
    except Exception as exc:
        log("error", "geocoding_failed", error=str(exc))
        if contact_id:
            ghl_create_task(
                contact_id,
                "Heat Map — geocoding failed",
                f"Could not geocode address '{address}' for {business_name}. "
                f"Manual intervention required.",
            )
        return {"status": "error", "code": "GEOCODING_FAILED", "message": str(exc)}

    # --- Step 2: Start Local Falcon scan ---
    scan_id = None
    scan_data = None
    try:
        scan_id = start_local_falcon_scan(business_name, keyword, lat, lng, grid_size)
        log("info", "scan_started", scan_id=scan_id)

        # --- Step 3: Poll for completion ---
        scan_data = poll_local_falcon_scan(scan_id)
        log("info", "scan_complete", scan_id=scan_id)

    except TimeoutError as exc:
        log("error", "scan_timeout", scan_id=scan_id, error=str(exc))
        if contact_id:
            ghl_create_task(
                contact_id,
                "URGENT — Heat Map scan timed out",
                f"Local Falcon scan for {business_name} / {keyword} timed out. "
                f"Manual scan required in Local Falcon dashboard.",
            )
        return {"status": "error", "code": "SCAN_TIMEOUT"}

    except Exception as exc:
        log("error", "scan_failed", error=str(exc))
        if contact_id:
            ghl_create_task(
                contact_id,
                "URGENT — Heat Map scan failed",
                f"Local Falcon scan failed for {business_name} / {keyword}. "
                f"Error: {str(exc)[:200]}. Manual scan required.",
            )
        return {"status": "error", "code": "SCAN_FAILED", "message": str(exc)}

    # --- Step 4 & 5 & 6: Parse results + stats ---
    stats = parse_scan_results(scan_data, grid_size)
    positions = stats.pop("positions", [])

    log("info", "stats_calculated",
        avg_position=stats.get("avg_position"),
        pct_in_pack=stats.get("pct_in_pack"))

    # --- Step 7: Generate heat map PNG ---
    png_bytes = generate_heatmap_png(positions, grid_size, business_name, keyword)

    # --- Step 8: Upload PNG to S3 ---
    png_key = s3_key_png(contact_id or "anon", keyword, date_str)
    png_url = s3_upload(png_bytes, png_key, "image/png") if png_bytes else ""

    result = {
        "status":          "complete",
        "business_name":   business_name,
        "keyword":         keyword,
        "grid_size":       grid_size,
        "scan_id":         scan_id,
        "avg_position":    stats.get("avg_position"),
        "pct_in_pack":     stats.get("pct_in_pack"),
        "pct_top1":        stats.get("pct_top1"),
        "coverage_radius_miles": stats.get("coverage_radius_miles"),
        "png_url":         png_url,
        "agent":           "heatMapGenerator",
        "version":         CODE_VERSION,
    }

    # --- Step 9 (purchase) / Step 10 (deep_dive) / Step 11 (manual) ---
    if trigger_source == "purchase" and contact_id:
        # Generate PDF + update GHL
        pdf_bytes = generate_pdf_report(
            business_name, keyword, stats, png_bytes, png_url
        )
        pdf_key = s3_key_pdf(contact_id, keyword, date_str)
        pdf_url = s3_upload(pdf_bytes, pdf_key, "application/pdf") if pdf_bytes else ""

        # Update GHL contact
        ghl_update_contact(
            contact_id,
            fields=[
                {"key": "heat_map_url",     "field_value": pdf_url},
                {"key": "heat_map_png_url", "field_value": png_url},
                {"key": "heat_map_score",   "field_value": str(stats.get("pct_in_pack", 0))},
                {"key": "heat_map_date",    "field_value": now.isoformat()},
                {"key": "heat_map_keyword", "field_value": keyword},
            ],
            tags=["heat-map-delivered"],
        )

        ghl_move_stage(contact_id, business_name)
        result["pdf_url"] = pdf_url

    # --- CloudWatch log ---
    log("info", "heat_map_complete",
        contact_id_hash=_h(contact_id) if contact_id else "none",
        scan_id=scan_id,
        keyword=keyword,
        grid_size=grid_size,
        avg_position=stats.get("avg_position"),
        pct_in_pack=stats.get("pct_in_pack"),
        trigger=trigger_source,
        version=CODE_VERSION)

    return result


# ----------------------------------------------------------------
# AgentCore entrypoint
# ----------------------------------------------------------------
if _SDK:
    app = BedrockAgentCoreApp()

    @app.entrypoint
    def heat_map_generator(payload: dict) -> dict:
        try:
            return run_heat_map_generator(payload)
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
