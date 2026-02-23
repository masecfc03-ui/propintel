"""
PropIntel PDF Report Builder v2
Charts, tile grids, badges, comp tables — matches web report quality.
reportlab only (zero system deps, Render free tier compatible).
"""

import io
import logging
import urllib.parse
from datetime import datetime

from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    HRFlowable, PageBreak, KeepTogether
)
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT
from reportlab.graphics.shapes import Drawing, Rect, String, Line
from reportlab.graphics.charts.barcharts import HorizontalBarChart
from reportlab.graphics import renderPDF

log = logging.getLogger(__name__)

# ── PAGE SETUP ────────────────────────────────────────────────────────────────
PW, PH = letter          # 612 x 792 pts
LM = RM = 0.65 * inch
TM = BM = 0.65 * inch
W  = PW - LM - RM        # 7.2"

# ── PALETTE ───────────────────────────────────────────────────────────────────
BLUE       = colors.HexColor("#2563eb")
BLUE_LIGHT = colors.HexColor("#dbeafe")
DARK       = colors.HexColor("#0f172a")
MID        = colors.HexColor("#334155")
GRAY       = colors.HexColor("#64748b")
GHOST      = colors.HexColor("#f1f5f9")
BORDER     = colors.HexColor("#e2e8f0")
WHITE      = colors.white
GREEN      = colors.HexColor("#16a34a")
GREEN_BG   = colors.HexColor("#dcfce7")
AMBER      = colors.HexColor("#d97706")
AMBER_BG   = colors.HexColor("#fef9c3")
RED_       = colors.HexColor("#dc2626")
RED_BG     = colors.HexColor("#fee2e2")
TEAL       = colors.HexColor("#0891b2")
PURPLE     = colors.HexColor("#7c3aed")
ORANGE     = colors.HexColor("#ea580c")


# ── STYLES ────────────────────────────────────────────────────────────────────
def S():
    return {
        "h1":      ParagraphStyle("h1",   fontName="Helvetica-Bold", fontSize=22, textColor=DARK, leading=26, spaceAfter=2),
        "h2":      ParagraphStyle("h2",   fontName="Helvetica-Bold", fontSize=13, textColor=DARK, leading=16, spaceBefore=14, spaceAfter=6),
        "h3":      ParagraphStyle("h3",   fontName="Helvetica-Bold", fontSize=9,  textColor=BLUE, leading=11, spaceBefore=12, spaceAfter=4),
        "sub":     ParagraphStyle("sub",  fontName="Helvetica",      fontSize=10, textColor=GRAY, leading=13, spaceAfter=10),
        "lbl":     ParagraphStyle("lbl",  fontName="Helvetica-Bold", fontSize=8,  textColor=GRAY, leading=11),
        "val":     ParagraphStyle("val",  fontName="Helvetica",      fontSize=9,  textColor=DARK, leading=12),
        "sm":      ParagraphStyle("sm",   fontName="Helvetica",      fontSize=7,  textColor=GRAY, leading=10),
        "note":    ParagraphStyle("note", fontName="Helvetica-Oblique", fontSize=7, textColor=GRAY, leading=10),
        "tile_v":  ParagraphStyle("tv",   fontName="Helvetica-Bold", fontSize=16, textColor=DARK, alignment=TA_CENTER, leading=20),
        "tile_l":  ParagraphStyle("tl",   fontName="Helvetica-Bold", fontSize=7,  textColor=GRAY, alignment=TA_CENTER, leading=9),
        "tile_s":  ParagraphStyle("ts",   fontName="Helvetica",      fontSize=8,  textColor=MID,  alignment=TA_CENTER, leading=10),
        "verdict": ParagraphStyle("vrd",  fontName="Helvetica-Bold", fontSize=18, textColor=GREEN, alignment=TA_CENTER, leading=22),
        "badge_g": ParagraphStyle("bg",   fontName="Helvetica-Bold", fontSize=7,  textColor=GREEN,  alignment=TA_CENTER),
        "badge_y": ParagraphStyle("by",   fontName="Helvetica-Bold", fontSize=7,  textColor=AMBER,  alignment=TA_CENTER),
        "badge_r": ParagraphStyle("br",   fontName="Helvetica-Bold", fontSize=7,  textColor=RED_,   alignment=TA_CENTER),
        "badge_b": ParagraphStyle("bb",   fontName="Helvetica-Bold", fontSize=7,  textColor=BLUE,   alignment=TA_CENTER),
        "footer":  ParagraphStyle("ft",   fontName="Helvetica",      fontSize=7,  textColor=GRAY, alignment=TA_CENTER, leading=10),
        "comp_h":  ParagraphStyle("ch",   fontName="Helvetica-Bold", fontSize=8,  textColor=DARK, alignment=TA_CENTER),
        "comp_v":  ParagraphStyle("cv",   fontName="Helvetica",      fontSize=8,  textColor=DARK),
        "comp_s":  ParagraphStyle("cs",   fontName="Helvetica",      fontSize=7,  textColor=GRAY),
    }


# ── HELPERS ───────────────────────────────────────────────────────────────────

def _hr(color=BORDER, thickness=0.5, space=8):
    return HRFlowable(width=W, thickness=thickness, color=color,
                      spaceBefore=space, spaceAfter=space)


def _section(title, s):
    return Paragraph(title.upper(), s["h3"])


def _kv_table(rows, s, col=(2.1*inch, 5.0*inch)):
    """Two-column label/value table."""
    data = []
    for lbl, val in rows:
        v = str(val) if val is not None and val != "" else "—"
        if v in ("None", "False", "True"):
            v = {"None": "—", "False": "No", "True": "Yes"}[v]
        data.append([Paragraph(lbl, s["lbl"]), Paragraph(v, s["val"])])
    if not data:
        return None
    t = Table(data, colWidths=col)
    t.setStyle(TableStyle([
        ("ROWBACKGROUNDS", (0,0),(-1,-1), [WHITE, GHOST]),
        ("GRID",           (0,0),(-1,-1), 0.4, BORDER),
        ("LEFTPADDING",    (0,0),(-1,-1), 7),
        ("RIGHTPADDING",   (0,0),(-1,-1), 7),
        ("TOPPADDING",     (0,0),(-1,-1), 5),
        ("BOTTOMPADDING",  (0,0),(-1,-1), 5),
        ("VALIGN",         (0,0),(-1,-1), "TOP"),
    ]))
    return t


def _tile(value, label, sub, s, accent=BLUE):
    """Single metric tile."""
    inner = Table([
        [Paragraph(str(value), s["tile_v"])],
        [Paragraph(label, s["tile_l"])],
        [Paragraph(sub,   s["tile_s"])],
    ], colWidths=[W/4 - 0.15*inch])
    inner.setStyle(TableStyle([
        ("BACKGROUND",    (0,0),(-1,-1), WHITE),
        ("BOX",           (0,0),(-1,-1), 1.5, accent),
        ("TOPPADDING",    (0,0),(-1,-1), 10),
        ("BOTTOMPADDING", (0,0),(-1,-1), 10),
        ("LEFTPADDING",   (0,0),(-1,-1), 6),
        ("RIGHTPADDING",  (0,0),(-1,-1), 6),
        ("ALIGN",         (0,0),(-1,-1), "CENTER"),
        ("LINEBELOW",     (0,0),(-1,0),  3, accent),
    ]))
    return inner


def _tile_row(tiles):
    """Row of 4 metric tiles."""
    row = Table([tiles], colWidths=[W/4]*4)
    row.setStyle(TableStyle([
        ("LEFTPADDING",  (0,0),(-1,-1), 3),
        ("RIGHTPADDING", (0,0),(-1,-1), 3),
        ("TOPPADDING",   (0,0),(-1,-1), 0),
        ("BOTTOMPADDING",(0,0),(-1,-1), 0),
    ]))
    return row


def _badge(text, s, bg, fg_style):
    """Colored pill badge."""
    t = Table([[Paragraph(text, fg_style)]], colWidths=[None])
    t.setStyle(TableStyle([
        ("BACKGROUND",    (0,0),(-1,-1), bg),
        ("ROUNDEDCORNERS",[4]),
        ("LEFTPADDING",   (0,0),(-1,-1), 6),
        ("RIGHTPADDING",  (0,0),(-1,-1), 6),
        ("TOPPADDING",    (0,0),(-1,-1), 3),
        ("BOTTOMPADDING", (0,0),(-1,-1), 3),
    ]))
    return t


def _bar_chart(labels, values, colors_list, width=W, height=1.6*inch, title=""):
    """Horizontal bar chart using reportlab graphics."""
    drawing = Drawing(width, height)
    if not values or all(v == 0 for v in values):
        return None

    chart = HorizontalBarChart()
    chart.x           = 130
    chart.y           = 10
    chart.width       = width - 160
    chart.height      = height - 20
    chart.data        = [values]
    chart.categoryAxis.categoryNames = labels
    chart.categoryAxis.labels.fontSize   = 8
    chart.categoryAxis.labels.fillColor  = GRAY
    chart.categoryAxis.labels.dx         = -4
    chart.valueAxis.labels.fontSize      = 8
    chart.valueAxis.labels.fillColor     = GRAY
    chart.valueAxis.labelTextFormat     = lambda v: f"${v/1e6:.1f}M" if v >= 1e6 else f"${v/1e3:.0f}K"
    chart.bars[0].fillColor   = colors_list[0] if colors_list else BLUE
    chart.bars[0].strokeColor = None
    chart.barWidth = chart.height / (len(labels) * 1.6) if labels else 12

    if title:
        drawing.add(String(0, height - 10, title,
                           fontName="Helvetica-Bold", fontSize=8, fillColor=GRAY))
    drawing.add(chart)
    return drawing


def _progress_bar(label, pct, color, width=W*0.48, s=None):
    """Labeled progress bar for demographics."""
    bar_w = width - 80
    bar_h = 10
    d = Drawing(width, 26)
    # background
    d.add(Rect(80, 8, bar_w, bar_h, fillColor=GHOST, strokeColor=None))
    # fill
    fill_w = bar_w * min(pct / 100.0, 1.0)
    d.add(Rect(80, 8, fill_w, bar_h, fillColor=color, strokeColor=None))
    # label
    d.add(String(0, 10, label[:22], fontName="Helvetica", fontSize=7, fillColor=GRAY))
    # pct
    d.add(String(80 + bar_w + 4, 10, f"{pct:.1f}%", fontName="Helvetica-Bold", fontSize=7, fillColor=color))
    return d


def _motivation_bar(score, width=W, s=None):
    """0-100 score bar with color gradient zones."""
    h = 28
    d = Drawing(width, h)
    seg_w = width / 3.0
    # Zones
    d.add(Rect(0,       10, seg_w,   14, fillColor=RED_BG,   strokeColor=None))
    d.add(Rect(seg_w,   10, seg_w,   14, fillColor=AMBER_BG, strokeColor=None))
    d.add(Rect(seg_w*2, 10, seg_w,   14, fillColor=GREEN_BG, strokeColor=None))
    # Labels
    d.add(String(seg_w*0.5 - 10, 12, "LOW",    fontName="Helvetica", fontSize=7, fillColor=RED_))
    d.add(String(seg_w*1.5 - 14, 12, "MEDIUM", fontName="Helvetica", fontSize=7, fillColor=AMBER))
    d.add(String(seg_w*2.5 - 10, 12, "HIGH",   fontName="Helvetica", fontSize=7, fillColor=GREEN))
    # Needle
    needle_x = (score / 100.0) * width
    d.add(Rect(needle_x - 3, 6, 6, 22, fillColor=DARK, strokeColor=None))
    d.add(String(needle_x - 8, 0, str(score), fontName="Helvetica-Bold", fontSize=8, fillColor=DARK))
    return d


# ── COMP TABLE ────────────────────────────────────────────────────────────────

def _comp_table(comps, s):
    """Styled sold-comps table. Accepts ATTOM comp dicts."""
    if not comps:
        return None
    header = ["Address", "Sold Date", "Sale Price", "Bldg SF", "$/SF", "Type"]
    rows = [header]
    for c in comps[:12]:
        days = c.get("days_ago")
        date_str = c.get("sale_date", "")[:10] if c.get("sale_date") else "—"
        if days is not None:
            date_str = f"{date_str} ({days}d ago)" if days else date_str
        rows.append([
            c.get("address", "—")[:30],
            date_str,
            c.get("sale_fmt", "—"),
            c.get("sf_fmt") or (f"{int(c['building_sf']):,} SF" if c.get("building_sf") else "—"),
            c.get("psf_fmt", "—"),
            (c.get("use_type") or "—")[:12],
        ])

    col_w = [2.4*inch, 1.1*inch, 0.95*inch, 0.75*inch, 0.7*inch, 0.85*inch]
    t = Table(rows, colWidths=col_w, repeatRows=1)
    t.setStyle(TableStyle([
        # Header
        ("BACKGROUND",    (0,0),(-1,0),  BLUE),
        ("TEXTCOLOR",     (0,0),(-1,0),  WHITE),
        ("FONTNAME",      (0,0),(-1,0),  "Helvetica-Bold"),
        ("FONTSIZE",      (0,0),(-1,0),  7),
        ("ALIGN",         (0,0),(-1,0),  "CENTER"),
        # Body
        ("ROWBACKGROUNDS",(0,1),(-1,-1), [WHITE, GHOST]),
        ("FONTNAME",      (0,1),(-1,-1), "Helvetica"),
        ("FONTSIZE",      (0,1),(-1,-1), 7.5),
        ("TEXTCOLOR",     (0,1),(-1,-1), DARK),
        # Grid
        ("GRID",          (0,0),(-1,-1), 0.4, BORDER),
        ("LEFTPADDING",   (0,0),(-1,-1), 5),
        ("RIGHTPADDING",  (0,0),(-1,-1), 5),
        ("TOPPADDING",    (0,0),(-1,-1), 4),
        ("BOTTOMPADDING", (0,0),(-1,-1), 4),
        ("VALIGN",        (0,0),(-1,-1), "MIDDLE"),
        # Highlight price column
        ("FONTNAME",      (2,1),( 2,-1), "Helvetica-Bold"),
        ("TEXTCOLOR",     (2,1),( 2,-1), BLUE),
    ]))
    return t


# ── MAIN GENERATOR ────────────────────────────────────────────────────────────

def generate_pdf_bytes(report: dict) -> bytes:
    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=letter,
        rightMargin=RM, leftMargin=LM,
        topMargin=TM,   bottomMargin=BM,
        title="PropIntel Report",
    )

    s   = S()
    e   = []   # elements

    # ── data shortcuts ──────────────────────────────────────────────────────
    p    = report.get("parcel", {}) or {}
    mkt  = report.get("market_estimate", {}) or {}
    fin  = report.get("financials", {}) or {}
    fld  = report.get("flood", {}) or {}
    dem  = report.get("demographics", {}) or {}
    mot  = report.get("motivation", {}) or {}
    da   = report.get("deal_analysis", {}) or {}
    ent  = report.get("owner_entity", {}) or {}
    sk   = report.get("skip_trace", {}) or {}
    flgs = report.get("flags", []) or []
    avm  = report.get("avm", {}) or {}
    comps_data   = report.get("sold_comps", {}) or {}
    mort         = report.get("mortgage", {}) or {}
    hist         = report.get("ownership_history", {}) or {}
    tier         = report.get("tier", "starter")
    full_addr    = (p.get("property_address") or report.get("input", "Unknown")) + \
                   (f", {p.get('county','').title()} County TX" if p.get("county") else "")
    gen          = (report.get("generated_at") or datetime.utcnow().isoformat())[:10]
    is_pro       = tier == "pro"

    # ════════════════════════════════════════════════════════════════════════
    # PAGE 1 — HEADER + BADGES + KEY METRICS + SIGNALS
    # ════════════════════════════════════════════════════════════════════════

    # ── Brand header ─────────────────────────────────────────────────────────
    hdr = Table([[
        Paragraph("PropIntel", s["h1"]),
        Paragraph(
            ('<font color="#2563eb">PRO</font>' if is_pro else 'STARTER') + f" Report  ·  {gen}",
            ParagraphStyle("rh", fontName="Helvetica", fontSize=9, textColor=GRAY,
                           alignment=TA_RIGHT, leading=12)
        )
    ]], colWidths=[W*0.6, W*0.4])
    hdr.setStyle(TableStyle([("VALIGN",(0,0),(-1,-1),"BOTTOM"),
                              ("LEFTPADDING",(0,0),(-1,-1),0),
                              ("RIGHTPADDING",(0,0),(-1,-1),0)]))
    e.append(hdr)
    e.append(_hr(BLUE, 2, 6))

    # ── Address block + badges ────────────────────────────────────────────────
    badges = []
    if p.get("absentee_owner"):
        badges.append(("ABSENTEE OWNER", AMBER_BG, s["badge_y"]))
    if p.get("out_of_state_owner"):
        badges.append(("OUT OF STATE", RED_BG, s["badge_r"]))
    if ent.get("entity_name") and not ent.get("is_individual"):
        badges.append(("ENTITY OWNER", BLUE_LIGHT, s["badge_b"]))
    if p.get("tax_delinquent"):
        badges.append(("TAX DELINQUENT", RED_BG, s["badge_r"]))

    badge_row_items = [_badge(txt, s, bg, st) for txt, bg, st in badges]
    addr_block = Table([[
        Paragraph(full_addr, ParagraphStyle("ab", fontName="Helvetica-Bold",
                                            fontSize=13, textColor=DARK, leading=16)),
    ]], colWidths=[W])
    addr_block.setStyle(TableStyle([
        ("BACKGROUND",    (0,0),(-1,-1), GHOST),
        ("LEFTPADDING",   (0,0),(-1,-1), 12),
        ("TOPPADDING",    (0,0),(-1,-1), 10),
        ("BOTTOMPADDING", (0,0),(-1,-1), 10),
        ("BOX",           (0,0),(-1,-1), 0.5, BORDER),
    ]))
    e.append(addr_block)

    if badge_row_items:
        padded = badge_row_items + [Spacer(0.1,0.1)] * (4 - len(badge_row_items))
        br = Table([padded[:4]], colWidths=[W/4]*4)
        br.setStyle(TableStyle([("LEFTPADDING",(0,0),(-1,-1),3),
                                 ("RIGHTPADDING",(0,0),(-1,-1),3),
                                 ("TOPPADDING",(0,0),(-1,-1),4),
                                 ("BOTTOMPADDING",(0,0),(-1,-1),0)]))
        e.append(br)

    e.append(Spacer(1, 10))

    # ── Key Metrics tiles (4-up) ──────────────────────────────────────────────
    assessed = p.get("assessed_total", 0)
    assessed_fmt = f"${assessed/1e3:.0f}K" if assessed and assessed < 1e6 else \
                   (f"${assessed/1e6:.2f}M" if assessed else "N/A")
    mkt_range    = mkt.get("range_fmt") or "Pending ATTOM"
    bldg_sf      = f"{int(p['building_sf']):,}" if p.get("building_sf") else "N/A"
    yr_built     = str(p.get("year_built", "N/A"))
    apn          = p.get("apn", "N/A")
    flood_zone   = fld.get("zone", "N/A")
    use_type     = (p.get("use_description") or "N/A")[:20]

    tiles = [
        _tile(assessed_fmt,   "COUNTY ASSESSED",   "Tax value (not market)", s, BLUE),
        _tile(mkt_range,      "EST. MARKET RANGE",  mkt.get("confidence",""), s, TEAL),
        _tile(bldg_sf + " SF","BUILDING SIZE",       f"Built {yr_built}", s, PURPLE),
        _tile(flood_zone,     "FEMA FLOOD ZONE",     "Minimal risk" if "X" in flood_zone else "Verify risk", s,
              GREEN if "X" in flood_zone else RED_),
    ]
    e.append(_tile_row(tiles))
    e.append(Spacer(1, 12))

    # ── Investment Signals ────────────────────────────────────────────────────
    if flgs:
        e.append(_section("Investment Signals", s))
        sig_rows = []
        for f_ in flgs:
            t_ = f_.get("type", "green")
            icon = "✓" if t_ == "green" else ("⚠" if t_ == "yellow" else "✗")
            clr  = GREEN if t_ == "green" else (AMBER if t_ == "yellow" else RED_)
            bg_  = GREEN_BG if t_ == "green" else (AMBER_BG if t_ == "yellow" else RED_BG)
            style = ParagraphStyle("fs", fontName="Helvetica", fontSize=8.5,
                                   textColor=clr, leading=12)
            sig_rows.append([Paragraph(f"{icon}  {f_.get('text','')}", style)])
        st = Table(sig_rows, colWidths=[W])
        st.setStyle(TableStyle([
            ("ROWBACKGROUNDS", (0,0),(-1,-1), [WHITE, GHOST]),
            ("GRID",           (0,0),(-1,-1), 0.4, BORDER),
            ("LEFTPADDING",    (0,0),(-1,-1), 10),
            ("TOPPADDING",     (0,0),(-1,-1), 6),
            ("BOTTOMPADDING",  (0,0),(-1,-1), 6),
        ]))
        e.append(st)
        e.append(Spacer(1, 8))

    # ── Motivation Score (visual bar) ─────────────────────────────────────────
    if mot.get("score") is not None:
        e.append(_section("Seller Motivation Score", s))
        score = mot.get("score", 0)
        tier_ = mot.get("tier", "LOW")
        score_color = GREEN if tier_ == "HIGH" else (AMBER if tier_ == "MEDIUM" else RED_)
        score_bg    = GREEN_BG if tier_ == "HIGH" else (AMBER_BG if tier_ == "MEDIUM" else RED_BG)

        score_tbl = Table([[
            Paragraph(str(score), ParagraphStyle("sc", fontName="Helvetica-Bold",
                      fontSize=32, textColor=score_color, alignment=TA_CENTER, leading=36)),
            Table([
                [Paragraph(f"<b>{tier_}</b>", ParagraphStyle("ti", fontName="Helvetica-Bold",
                            fontSize=11, textColor=score_color, leading=14))],
                [Paragraph(mot.get("interpretation","")[:160],
                            ParagraphStyle("in", fontName="Helvetica", fontSize=8,
                                           textColor=MID, leading=12))],
            ], colWidths=[W - 1.1*inch]),
        ]], colWidths=[1.0*inch, W - 1.1*inch])
        score_tbl.setStyle(TableStyle([
            ("BACKGROUND",    (0,0),(-1,-1), score_bg),
            ("BOX",           (0,0),(-1,-1), 1, score_color),
            ("LEFTPADDING",   (0,0),(-1,-1), 10),
            ("TOPPADDING",    (0,0),(-1,-1), 10),
            ("BOTTOMPADDING", (0,0),(-1,-1), 10),
            ("VALIGN",        (0,0),(-1,-1), "MIDDLE"),
        ]))
        e.append(score_tbl)

        # Score bar
        e.append(Spacer(1,5))
        e.append(_motivation_bar(score, W))

        # Indicators table
        indicators = mot.get("indicators", [])
        if indicators:
            e.append(Spacer(1, 8))
            ind_rows = [["Signal", "Triggered", "Points", "Evidence"]]
            for ind in indicators:
                triggered = "✓ YES" if ind.get("triggered") else "✗ No"
                pts       = f"+{ind['points']}" if ind.get("triggered") else "0"
                ind_rows.append([
                    ind.get("name", "")[:30],
                    triggered,
                    pts,
                    ind.get("evidence", "")[:55],
                ])
            ind_t = Table(ind_rows, colWidths=[2.2*inch, 0.75*inch, 0.55*inch, 3.7*inch],
                          repeatRows=1)
            ind_t.setStyle(TableStyle([
                ("BACKGROUND",    (0,0),(-1,0),  MID),
                ("TEXTCOLOR",     (0,0),(-1,0),  WHITE),
                ("FONTNAME",      (0,0),(-1,0),  "Helvetica-Bold"),
                ("FONTSIZE",      (0,0),(-1,0),  7),
                ("ROWBACKGROUNDS",(0,1),(-1,-1), [WHITE, GHOST]),
                ("FONTSIZE",      (0,1),(-1,-1), 7.5),
                ("GRID",          (0,0),(-1,-1), 0.4, BORDER),
                ("LEFTPADDING",   (0,0),(-1,-1), 5),
                ("TOPPADDING",    (0,0),(-1,-1), 4),
                ("BOTTOMPADDING", (0,0),(-1,-1), 4),
                ("TEXTCOLOR",     (1,1),(1,-1),  GREEN),
                ("FONTNAME",      (1,1),(1,-1),  "Helvetica-Bold"),
            ]))
            e.append(ind_t)

    # ════════════════════════════════════════════════════════════════════════
    # PAGE 2 — PARCEL + VALUATION + BAR CHART
    # ════════════════════════════════════════════════════════════════════════
    e.append(PageBreak())

    # ── Parcel & Ownership ────────────────────────────────────────────────────
    e.append(Paragraph("Parcel & Ownership", s["h2"]))
    e.append(_hr())
    rows = [
        ("Owner of Record",       p.get("owner_name")),
        ("APN / Parcel ID",       p.get("apn")),
        ("Property Address",      p.get("property_address")),
        ("County / State",        f"{(p.get('county') or '').title()}, {p.get('state','')}"),
        ("Use Description",       p.get("use_description")),
        ("Zoning",                p.get("zoning")),
        ("Building SF",           f"{int(p['building_sf']):,} SF" if p.get("building_sf") else None),
        ("Lot (Acres)",           p.get("lot_acres")),
        ("Year Built",            p.get("year_built")),
        ("Owner Mailing Address", p.get("owner_mailing")),
        ("Owner City / State",    f"{p.get('owner_city','')}, {p.get('owner_state','')}" if p.get("owner_city") else None),
        ("Absentee Owner",        "YES — mailing address differs from property" if p.get("absentee_owner") else "No"),
        ("Out-of-State Owner",    "YES" if p.get("out_of_state_owner") else "No"),
        ("Tax Delinquent",        "YES — Contact county for resolution" if p.get("tax_delinquent") else "No"),
        ("Data Source",           p.get("data_sources") or p.get("source")),
    ]
    t = _kv_table([(l,v) for l,v in rows if v is not None], s)
    if t: e.append(t)

    # ── Valuation ─────────────────────────────────────────────────────────────
    e.append(Spacer(1, 14))
    e.append(Paragraph("Valuation", s["h2"]))
    e.append(_hr())

    val_rows = [
        ("County Assessed Total (Tax Value)", f"${assessed:,.0f}" if assessed else None),
        ("  → Assessed Land",                 f"${p.get('assessed_land',0):,.0f}" if p.get("assessed_land") else None),
        ("  → Assessed Improvements",         f"${p.get('assessed_improvement',0):,.0f}" if p.get("assessed_improvement") else None),
        ("Est. Market Range",                 mkt.get("range_fmt")),
        ("Market Methodology",                mkt.get("methodology") or mkt.get("note")),
        ("AVM Confidence",                    mkt.get("confidence")),
        ("AVM Date",                          mkt.get("avm_date") or avm.get("calc_date")),
    ]
    t = _kv_table([(l,v) for l,v in val_rows if v is not None], s)
    if t: e.append(t)

    # Bar chart: land vs improvement vs market estimate
    land_val  = p.get("assessed_land", 0) or 0
    impr_val  = p.get("assessed_improvement", 0) or 0
    mkt_mid   = mkt.get("market_low") or (assessed * 1.05 if assessed else 0)
    chart_labels = ["Market Est.", "Assessed Total", "Improvements", "Land Value"]
    chart_values = [mkt_mid, assessed or 0, impr_val, land_val]
    if any(v > 0 for v in chart_values):
        e.append(Spacer(1, 10))
        e.append(Paragraph("VALUE COMPARISON", s["h3"]))
        chart = _bar_chart(chart_labels, chart_values,
                           [TEAL, BLUE, PURPLE, ORANGE], W, 1.8*inch)
        if chart:
            e.append(chart)

    # ════════════════════════════════════════════════════════════════════════
    # PAGE 3 — FINANCIALS + DEAL ANALYSIS + MORTGAGE
    # ════════════════════════════════════════════════════════════════════════
    e.append(PageBreak())

    e.append(Paragraph("Financial Estimates", s["h2"]))
    e.append(_hr())

    if fin.get("available"):
        # Financial tiles (2-row grid)
        fin_tiles_data = [
            (f"${fin.get('est_annual_tax',0):,.0f}",  "EST. ANNUAL TAX",      f"{fin.get('tax_rate_pct',0)}% eff. rate",  BLUE),
            (f"${fin.get('est_monthly_tax',0):,.0f}", "EST. MONTHLY TAX",     "Based on county avg",                      TEAL),
        ]
        if fin.get("cash_flow"):
            fin_tiles_data += [
                (fin.get("gsi_range","—"),   "GROSS INCOME (GSI)", fin.get("rent_use_label",""),                           PURPLE),
                (fin.get("noi_range","—"),   "NET OPER. INCOME",   "65% expense ratio",                                   ORANGE),
            ]
        if len(fin_tiles_data) >= 4:
            row1 = [_tile(v, l, sb, s, ac) for v, l, sb, ac in fin_tiles_data[:4]]
            e.append(_tile_row(row1))
        else:
            row1 = [_tile(v, l, sb, s, ac) for v, l, sb, ac in fin_tiles_data]
            while len(row1) < 4:
                row1.append(Spacer(0.1, 0.1))
            e.append(_tile_row(row1))

        if fin.get("cash_flow") and fin.get("implied_cap_rate"):
            e.append(Spacer(1, 6))
            cap_tile = [
                _tile(f"{fin['implied_cap_rate']}%",       "IMPLIED CAP RATE",  "At est. market mid-value", s, TEAL),
                _tile(fin.get("rent_per_sf_range","—"),    "MARKET RENT RANGE", f"Per SF/yr — {fin.get('rent_use_label','')}", s, BLUE),
                _tile(f"{int(fin.get('building_sf',0)):,} SF" if fin.get("building_sf") else "N/A", "BUILDING SF", "From county records", s, PURPLE),
                Spacer(0.1, 0.1),
            ]
            e.append(_tile_row(cap_tile))

        e.append(Spacer(1, 8))
        notes = []
        if fin.get("tax_note"):      notes.append(fin["tax_note"])
        if fin.get("cash_flow_note"):notes.append(fin["cash_flow_note"])
        if notes:
            for n in notes:
                e.append(Paragraph(f"⚠  {n}", s["note"]))
    else:
        e.append(Paragraph("Financial estimates not available for this property type.", s["sm"]))

    # ── Deal Analysis ─────────────────────────────────────────────────────────
    e.append(Spacer(1, 12))
    e.append(Paragraph("Deal Analysis", s["h2"]))
    e.append(_hr())

    loan = da.get("loan_assumptions") or {}
    da_rows = [
        ("Asking Price",              da.get("asking_price_fmt") or "Not listed / not available"),
        ("Price per SF",              f"${da.get('price_per_sf'):,.0f}/SF" if da.get("price_per_sf") else "N/A"),
        ("Building SF",               f"{int(da['building_sf']):,} SF" if da.get("building_sf") else None),
        ("Assessed vs Asking",        f"+{da.get('assessed_vs_asking_premium_pct')}% premium to assessed" if da.get("assessed_vs_asking_premium_pct") else "N/A"),
        ("Stated Cap Rate",           f"{da.get('stated_cap_rate')}%" if da.get("stated_cap_rate") else "Not available"),
        ("Stated NOI",                da.get("stated_noi_fmt") or "Not available"),
        ("DSCR (Debt Service)",       f"{da.get('dscr'):.2f}x" if da.get("dscr") else "N/A — enter asking price to calculate"),
        ("Cash-on-Cash Return",       f"{da.get('cash_on_cash_pct')}%" if da.get("cash_on_cash_pct") else "N/A"),
        ("Monthly Debt Service",      f"${da.get('monthly_debt_service'):,.0f}/mo" if da.get("monthly_debt_service") else "N/A"),
        ("Assumed LTV",               f"{loan.get('ltv_pct',75)}% (default)"),
        ("Assumed Rate",              f"{loan.get('rate_pct',7.0)}% (default)"),
        ("Amortization",              f"{loan.get('amortization_years',25)} yrs (default)"),
    ]
    t = _kv_table([(l,v) for l,v in da_rows if v is not None], s)
    if t: e.append(t)
    if da.get("note"):
        e.append(Spacer(1,4))
        e.append(Paragraph(f"⚠  {da['note']}", s["note"]))

    # ── Mortgage & Lien ───────────────────────────────────────────────────────
    e.append(Spacer(1, 12))
    e.append(Paragraph("Mortgage & Lien Data", s["h2"]))
    e.append(_hr())
    if mort.get("available"):
        mort_rows = [
            ("Loan Amount",        mort.get("loan_amount_fmt")),
            ("Loan Type",          mort.get("loan_type")),
            ("Interest Rate",      f"{mort.get('interest_rate')}%" if mort.get("interest_rate") else None),
            ("Lender",             mort.get("lender_name")),
            ("Maturity Date",      mort.get("maturity_date")),
            ("Open Lien Count",    str(mort.get("open_lien_count")) if mort.get("open_lien_count") is not None else None),
            ("Open Lien Balance",  mort.get("open_lien_fmt")),
            ("Last Sale Price",    mort.get("last_sale_price_fmt")),
            ("Last Sale Date",     mort.get("last_sale_date")),
        ]
        t = _kv_table([(l,v) for l,v in mort_rows if v is not None], s)
        if t: e.append(t)
    else:
        e.append(Paragraph(
            "Mortgage & lien data activates when ATTOM API is configured. "
            "Includes: open loan balance, lender name, loan type, open lien count, last purchase price and date.",
            s["sm"]
        ))

    # ════════════════════════════════════════════════════════════════════════
    # PAGE 4 — SOLD COMPS
    # ════════════════════════════════════════════════════════════════════════
    e.append(PageBreak())

    e.append(Paragraph("Sold Comparable Sales", s["h2"]))
    e.append(_hr())

    comps = comps_data.get("comps", [])
    comp_stats = comps_data.get("stats", {})

    if comps:
        # Stats tiles
        stat_tiles = [
            _tile(str(comp_stats.get("comp_count",0)),     "COMPS FOUND",    f"{comps_data.get('radius_miles',0.5)}mi radius", s, BLUE),
            _tile(comp_stats.get("median_price_fmt","—"),  "MEDIAN PRICE",   "Sold comps", s, TEAL),
            _tile(comp_stats.get("median_psf_fmt","—"),    "MEDIAN $/SF",    "All comps", s, PURPLE),
            _tile(comp_stats.get("price_range_fmt","—"),   "PRICE RANGE",    f"Last {comps_data.get('months_back',12)} months", s, ORANGE),
        ]
        e.append(_tile_row(stat_tiles))
        e.append(Spacer(1, 10))
        ct = _comp_table(comps, s)
        if ct: e.append(ct)
        e.append(Spacer(1, 6))
        e.append(Paragraph(f"Source: ATTOM · {comp_stats.get('comp_count',0)} sales within {comps_data.get('radius_miles',0.5)}mi · Last {comps_data.get('months_back',12)} months", s["sm"]))
    else:
        # Placeholder showing what will appear
        placeholder = Table([[
            Table([
                [Paragraph("📊 Sold Comps", ParagraphStyle("ph", fontName="Helvetica-Bold",
                            fontSize=13, textColor=BLUE, alignment=TA_CENTER))],
                [Paragraph("Coming with ATTOM API", ParagraphStyle("ph2", fontName="Helvetica",
                            fontSize=9, textColor=GRAY, alignment=TA_CENTER))],
                [Paragraph(
                    "Once ATTOM_API_KEY is set, this section will show:\n"
                    "• Up to 15 sold comparable properties within 0.5 miles\n"
                    "• 90-day, 6-month, and 12-month breakdowns\n"
                    "• Sale price, price/SF, days on market, property type\n"
                    "• Median and average comp statistics\n"
                    "• Price trend over time",
                    ParagraphStyle("ph3", fontName="Helvetica", fontSize=8.5,
                                   textColor=MID, leading=14, alignment=TA_CENTER)
                )],
            ], colWidths=[W - 1.5*inch])
        ]], colWidths=[W])
        placeholder.setStyle(TableStyle([
            ("BACKGROUND",    (0,0),(-1,-1), GHOST),
            ("BOX",           (0,0),(-1,-1), 1, BORDER),
            ("TOPPADDING",    (0,0),(-1,-1), 30),
            ("BOTTOMPADDING", (0,0),(-1,-1), 30),
            ("ALIGN",         (0,0),(-1,-1), "CENTER"),
            ("VALIGN",        (0,0),(-1,-1), "MIDDLE"),
        ]))
        e.append(placeholder)

    # Ownership History
    e.append(Spacer(1, 14))
    e.append(Paragraph("Ownership History", s["h2"]))
    e.append(_hr())
    history = hist.get("history", [])
    if history:
        if hist.get("hold_years"):
            e.append(Paragraph(f"Current owner has held this property for approximately <b>{hist['hold_years']} years</b>.", s["val"]))
            e.append(Spacer(1, 6))
        hist_rows = [["Sale Date", "Buyer", "Sale Price", "Document Type"]]
        for h_ in history[:8]:
            hist_rows.append([
                h_.get("sale_date", "—")[:10],
                (h_.get("buyer_name") or "—")[:30],
                h_.get("sale_fmt") or "—",
                (h_.get("document_type") or "—")[:25],
            ])
        ht = Table(hist_rows, colWidths=[1.1*inch, 2.5*inch, 1.2*inch, 2.4*inch], repeatRows=1)
        ht.setStyle(TableStyle([
            ("BACKGROUND",    (0,0),(-1,0),  MID),
            ("TEXTCOLOR",     (0,0),(-1,0),  WHITE),
            ("FONTNAME",      (0,0),(-1,0),  "Helvetica-Bold"),
            ("FONTSIZE",      (0,0),(-1,0),  7),
            ("ROWBACKGROUNDS",(0,1),(-1,-1), [WHITE, GHOST]),
            ("FONTNAME",      (0,1),(-1,-1), "Helvetica"),
            ("FONTSIZE",      (0,1),(-1,-1), 8),
            ("GRID",          (0,0),(-1,-1), 0.4, BORDER),
            ("LEFTPADDING",   (0,0),(-1,-1), 6),
            ("TOPPADDING",    (0,0),(-1,-1), 4),
            ("BOTTOMPADDING", (0,0),(-1,-1), 4),
        ]))
        e.append(ht)
    else:
        e.append(Paragraph(
            "Deed history activates with ATTOM API — shows every sale transaction, buyer names, prices, and dates going back 20+ years.",
            s["sm"]
        ))

    # ════════════════════════════════════════════════════════════════════════
    # PAGE 5 — OWNER INTEL + SKIP TRACE
    # ════════════════════════════════════════════════════════════════════════
    e.append(PageBreak())

    e.append(Paragraph("Owner Intelligence", s["h2"]))
    e.append(_hr())

    # Entity block
    entity_name = ent.get("entity_name", "")
    if entity_name and not ent.get("is_individual"):
        ent_rows = [
            ("Entity Name",      entity_name),
            ("TX SOS Status",    ent.get("status")),
            ("Formation Date",   ent.get("formation_date")),
            ("Registered Agent", ent.get("registered_agent")),
            ("TX SOS Record",    ent.get("manual_url")),
        ]
        t = _kv_table([(l,v) for l,v in ent_rows if v not in (None,"")], s)
        if t: e.append(t)
        if ent.get("error") and "403" in str(ent.get("error","")):
            e.append(Spacer(1,4))
            e.append(Paragraph("⚠  TX SOS lookup was blocked (bot protection). Search manually at the link above.", s["note"]))
    else:
        e.append(Paragraph(f"Owner: {entity_name or p.get('owner_name','Unknown')} (Individual)", s["val"]))

    # Skip trace
    e.append(Spacer(1, 14))
    e.append(Paragraph("Skip Trace / Owner Contact" + (" (Pro)" if is_pro else ""), s["h2"]))
    e.append(_hr())

    if is_pro:
        sk_status = sk.get("status", "")
        if sk_status == "hit":
            phones = sk.get("phones", [])
            emails = sk.get("emails", [])
            sk_rows = [
                ("Phone Numbers", ", ".join(phones[:5]) if phones else "None found"),
                ("Email Addresses", ", ".join(emails[:5]) if emails else "None found"),
                ("Source", sk.get("source")),
                ("Credits Used", str(sk.get("credits_used", 0))),
            ]
            t = _kv_table(sk_rows, s)
            if t: e.append(t)
        else:
            e.append(Paragraph(f"Status: {sk_status}", s["lbl"]))
            e.append(Spacer(1, 4))
            e.append(Paragraph(sk.get("note",""), s["sm"]))
    else:
        e.append(Paragraph("Owner contact data is available on the Pro plan ($29.99).", s["sm"]))

    # Liens
    e.append(Spacer(1, 14))
    e.append(Paragraph("Liens & Encumbrances", s["h2"]))
    e.append(_hr())
    liens = report.get("liens", {}) or {}
    lien_rows = [
        ("Status",      liens.get("status")),
        ("Note",        liens.get("note")),
        ("Manual URL",  liens.get("manual_url")),
        ("APN",         liens.get("apn")),
    ]
    t = _kv_table([(l,v) for l,v in lien_rows if v not in (None,"")], s)
    if t:
        e.append(t)
    else:
        e.append(Paragraph("Lien data not available. Verify manually via county clerk records.", s["sm"]))

    # ════════════════════════════════════════════════════════════════════════
    # PAGE 6 — FLOOD + DEMOGRAPHICS + RESEARCH LINKS
    # ════════════════════════════════════════════════════════════════════════
    e.append(PageBreak())

    # FEMA Flood
    e.append(Paragraph("FEMA Flood Zone Analysis", s["h2"]))
    e.append(_hr())

    zone_name = fld.get("zone","—")
    is_x      = "X" in zone_name.upper() if zone_name else False
    flood_tbl = Table([[
        Table([
            [Paragraph(zone_name, ParagraphStyle("fz", fontName="Helvetica-Bold",
                       fontSize=24, textColor=GREEN if is_x else RED_,
                       alignment=TA_CENTER, leading=28))],
            [Paragraph("FLOOD ZONE", ParagraphStyle("fzl", fontName="Helvetica-Bold",
                       fontSize=7, textColor=GRAY, alignment=TA_CENTER))],
        ], colWidths=[1.4*inch]),
        Table([
            [Paragraph(fld.get("description","No description available"),
                       ParagraphStyle("fd", fontName="Helvetica-Bold", fontSize=10,
                                      textColor=GREEN if is_x else RED_, leading=13))],
            [Paragraph(
                "No flood insurance required." if is_x else
                "⚠ Flood insurance likely REQUIRED. Verify with lender.",
                ParagraphStyle("fn", fontName="Helvetica", fontSize=8, textColor=MID, leading=11)
            )],
            [Paragraph(f"FIRM Panel: {fld.get('firm_panel','N/A')}  ·  Source: {fld.get('source','FEMA NFHL')}",
                       s["sm"])],
        ], colWidths=[W - 1.6*inch]),
    ]], colWidths=[1.5*inch, W - 1.6*inch])
    flood_tbl.setStyle(TableStyle([
        ("BACKGROUND",    (0,0),(-1,-1), GREEN_BG if is_x else RED_BG),
        ("BOX",           (0,0),(-1,-1), 1, GREEN if is_x else RED_),
        ("LEFTPADDING",   (0,0),(-1,-1), 10),
        ("TOPPADDING",    (0,0),(-1,-1), 12),
        ("BOTTOMPADDING", (0,0),(-1,-1), 12),
        ("VALIGN",        (0,0),(-1,-1), "MIDDLE"),
    ]))
    e.append(flood_tbl)

    # Demographics
    e.append(Spacer(1, 14))
    e.append(Paragraph("Neighborhood Demographics", s["h2"]))
    e.append(_hr())
    if dem.get("population"):
        dem_tiles = [
            _tile(f"{int(dem.get('population',0)):,}",             "POPULATION",        f"ZIP {dem.get('zip','')}",     s, BLUE),
            _tile(dem.get("median_household_income_fmt","—"),      "MEDIAN HH INCOME",   "Census ACS",                  s, TEAL),
            _tile(f"{dem.get('owner_occupied_pct',0):.0f}%",       "OWNER OCCUPIED",     "% of housing units",           s, PURPLE),
            _tile(str(dem.get("median_age","—")),                   "MEDIAN AGE",         "Years",                       s, ORANGE),
        ]
        e.append(_tile_row(dem_tiles))
        e.append(Spacer(1, 10))

        # Progress bars
        bars = []
        if dem.get("owner_occupied_pct") is not None:
            bars.append(_progress_bar("Owner Occupied", float(dem["owner_occupied_pct"]), BLUE))
        if dem.get("unemployment_rate") is not None:
            bars.append(_progress_bar("Unemployment", float(dem["unemployment_rate"]), RED_))

        if bars:
            bar_tbl = Table([[b] for b in bars], colWidths=[W])
            bar_tbl.setStyle(TableStyle([
                ("LEFTPADDING",  (0,0),(-1,-1), 0),
                ("TOPPADDING",   (0,0),(-1,-1), 2),
                ("BOTTOMPADDING",(0,0),(-1,-1), 2),
            ]))
            e.append(bar_tbl)

        e.append(Spacer(1, 6))
        e.append(Paragraph(f"Source: {dem.get('source','U.S. Census Bureau ACS')}  ·  ZIP {dem.get('zip','')}", s["sm"]))

    # Research Links
    e.append(Spacer(1, 14))
    e.append(Paragraph("Research & Comparables", s["h2"]))
    e.append(_hr())
    enc_addr = urllib.parse.quote(p.get("property_address","") + " " + p.get("county","") + " TX")
    apn_     = p.get("apn","")
    permit_  = report.get("permit_portal", {}) or {}

    link_rows = [
        ("Zillow Comps Search",   f"https://www.zillow.com/homes/{enc_addr}_rb/"),
        ("Redfin Comps Search",   f"https://www.redfin.com/TX/search#{urllib.parse.quote(p.get('property_address',''))}"),
        ("LoopNet (Commercial)",  f"https://www.loopnet.com/search/commercial-real-estate/{urllib.parse.quote((p.get('county','') + ' county tx').strip())}/for-sale/"),
        ("County Assessor Record",permit_.get("assessor","https://www.dallascad.org")),
        ("Permit Portal",         permit_.get("permit","https://www.dallascounty.org")),
        ("TX SOS Entity Search",  ent.get("manual_url","https://www.sos.state.tx.us")),
        ("FEMA Flood Map",        "https://msc.fema.gov/portal/home"),
        ("Census Data (ZIP)",     f"https://data.census.gov/cedsci/table?g=860XX00US{dem.get('zip','')}"),
    ]
    t = _kv_table(link_rows, s)
    if t: e.append(t)

    # ════════════════════════════════════════════════════════════════════════
    # DISCLAIMER
    # ════════════════════════════════════════════════════════════════════════
    e.append(Spacer(1, 20))
    e.append(_hr(BORDER, 0.5, 4))
    e.append(Paragraph(
        "This report is generated from publicly available data sources including county appraisal districts, "
        "FEMA National Flood Hazard Layer, U.S. Census Bureau ACS, Texas Secretary of State, and ATTOM Data Solutions. "
        "Financial estimates and deal analysis figures are projections based on market averages — not certified appraisals or verified financials. "
        "PropIntel is not a licensed real estate broker, appraiser, or attorney. Nothing in this report constitutes investment, legal, or financial advice. "
        f"Verify all data independently before making investment decisions.  ·  Generated {gen}  ·  propertyvalueintel.com",
        s["footer"]
    ))

    doc.build(e)
    return buf.getvalue()
