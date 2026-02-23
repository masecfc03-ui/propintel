"""
PropIntel PDF Report Builder v3 — MAXIMUM DATA
Every field, every section, every signal. No limits.
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
from reportlab.graphics.shapes import Drawing, Rect, String, Line, Circle
from reportlab.graphics.charts.barcharts import HorizontalBarChart
from reportlab.graphics.charts.piecharts import Pie

log = logging.getLogger(__name__)

PW, PH = letter
LM = RM = 0.60 * inch
TM = BM = 0.65 * inch
W  = PW - LM - RM   # ~7.3"

# ── PALETTE ──────────────────────────────────────────────────────────────────
BLUE       = colors.HexColor("#2563eb")
BLUE_D     = colors.HexColor("#1d4ed8")
BLUE_LIGHT = colors.HexColor("#dbeafe")
DARK       = colors.HexColor("#0f172a")
MID        = colors.HexColor("#334155")
GRAY       = colors.HexColor("#64748b")
GHOST      = colors.HexColor("#f8fafc")
GHOST2     = colors.HexColor("#f1f5f9")
BORDER     = colors.HexColor("#e2e8f0")
WHITE      = colors.white
GREEN      = colors.HexColor("#16a34a")
GREEN_BG   = colors.HexColor("#dcfce7")
GREEN_D    = colors.HexColor("#14532d")
AMBER      = colors.HexColor("#d97706")
AMBER_BG   = colors.HexColor("#fef9c3")
RED_       = colors.HexColor("#dc2626")
RED_BG     = colors.HexColor("#fee2e2")
TEAL       = colors.HexColor("#0891b2")
TEAL_BG    = colors.HexColor("#cffafe")
PURPLE     = colors.HexColor("#7c3aed")
PURPLE_BG  = colors.HexColor("#ede9fe")
ORANGE     = colors.HexColor("#ea580c")
ORANGE_BG  = colors.HexColor("#ffedd5")
SLATE      = colors.HexColor("#475569")


def _P(text, **kw):
    """Quick Paragraph helper."""
    base = dict(fontName="Helvetica", fontSize=9, textColor=DARK, leading=13)
    base.update(kw)
    return Paragraph(str(text) if text is not None else "—",
                     ParagraphStyle("_p", **base))


def _PB(text, **kw):
    kw["fontName"] = "Helvetica-Bold"
    return _P(text, **kw)


def _hr(color=BORDER, thick=0.5, before=6, after=6):
    return HRFlowable(width=W, thickness=thick, color=color,
                      spaceBefore=before, spaceAfter=after)


def _sec(title):
    """Section header — blue uppercase label."""
    return Paragraph(title.upper(),
                     ParagraphStyle("sec", fontName="Helvetica-Bold", fontSize=8,
                                    textColor=BLUE, spaceBefore=14, spaceAfter=5,
                                    leading=10))


def _h2(title):
    return Paragraph(title, ParagraphStyle("h2", fontName="Helvetica-Bold",
                     fontSize=14, textColor=DARK, leading=18, spaceBefore=6, spaceAfter=4))


def _v(val, fallback="—"):
    """Safe value formatter."""
    if val is None or val == "" or str(val).strip() == "":
        return fallback
    if isinstance(val, bool):
        return "Yes" if val else "No"
    return str(val)


def _kv(rows, col=(2.1*inch, 5.1*inch), alt=True):
    """Two-column label/value table."""
    data = []
    for lbl, val in rows:
        v = _v(val)
        data.append([
            _PB(lbl, fontSize=8, textColor=GRAY, leading=11),
            _P(v,  fontSize=9, textColor=DARK, leading=12),
        ])
    if not data:
        return None
    t = Table(data, colWidths=col)
    bgs = [WHITE, GHOST2] if alt else [WHITE, WHITE]
    t.setStyle(TableStyle([
        ("ROWBACKGROUNDS", (0,0),(-1,-1), bgs),
        ("GRID",           (0,0),(-1,-1), 0.4, BORDER),
        ("LEFTPADDING",    (0,0),(-1,-1), 7),
        ("RIGHTPADDING",   (0,0),(-1,-1), 7),
        ("TOPPADDING",     (0,0),(-1,-1), 5),
        ("BOTTOMPADDING",  (0,0),(-1,-1), 5),
        ("VALIGN",         (0,0),(-1,-1), "TOP"),
    ]))
    return t


def _tile4(items, s_dict):
    """Row of 4 metric tiles. items = [(val,label,sub,accent_color), ...]"""
    cells = []
    for val, label, sub, accent in items:
        inner = Table([
            [_PB(str(val), fontSize=15, textColor=DARK, alignment=TA_CENTER, leading=18)],
            [_PB(label,    fontSize=7,  textColor=GRAY, alignment=TA_CENTER, leading=9)],
            [_P(sub,       fontSize=7.5,textColor=MID,  alignment=TA_CENTER, leading=10)],
        ], colWidths=[W/4 - 0.12*inch])
        inner.setStyle(TableStyle([
            ("BACKGROUND",    (0,0),(-1,-1), WHITE),
            ("BOX",           (0,0),(-1,-1), 1.5, accent),
            ("LINEBELOW",     (0,0),(-1,0),  3,   accent),
            ("TOPPADDING",    (0,0),(-1,-1), 10),
            ("BOTTOMPADDING", (0,0),(-1,-1), 10),
            ("LEFTPADDING",   (0,0),(-1,-1), 4),
            ("RIGHTPADDING",  (0,0),(-1,-1), 4),
        ]))
        cells.append(inner)
    while len(cells) < 4:
        cells.append(Spacer(0.1, 0.1))
    row = Table([cells], colWidths=[W/4]*4)
    row.setStyle(TableStyle([
        ("LEFTPADDING",  (0,0),(-1,-1), 2),
        ("RIGHTPADDING", (0,0),(-1,-1), 2),
        ("TOPPADDING",   (0,0),(-1,-1), 0),
        ("BOTTOMPADDING",(0,0),(-1,-1), 0),
    ]))
    return row


def _badge_row(badges):
    """badges = [(text, bg_color, text_color), ...]"""
    cells = []
    for text, bg, fg in badges:
        t = Table([[_PB(text, fontSize=7, textColor=fg, alignment=TA_CENTER)]],
                  colWidths=[None])
        t.setStyle(TableStyle([
            ("BACKGROUND",    (0,0),(-1,-1), bg),
            ("LEFTPADDING",   (0,0),(-1,-1), 7),
            ("RIGHTPADDING",  (0,0),(-1,-1), 7),
            ("TOPPADDING",    (0,0),(-1,-1), 3),
            ("BOTTOMPADDING", (0,0),(-1,-1), 3),
            ("BOX",           (0,0),(-1,-1), 0.5, fg),
        ]))
        cells.append(t)
    if not cells:
        return None
    while len(cells) < 4:
        cells.append(Spacer(0.1, 0.1))
    r = Table([cells[:4]], colWidths=[W/4]*4)
    r.setStyle(TableStyle([
        ("TOPPADDING",   (0,0),(-1,-1), 4),
        ("LEFTPADDING",  (0,0),(-1,-1), 2),
        ("RIGHTPADDING", (0,0),(-1,-1), 2),
    ]))
    return r


def _hbar_chart(labels, values, bar_colors, width=W, height=2.0*inch):
    """Horizontal bar chart."""
    if not values or all(v == 0 for v in values):
        return None
    d = Drawing(width, height)
    chart = HorizontalBarChart()
    chart.x      = 145
    chart.y      = 15
    chart.width  = width - 170
    chart.height = height - 30
    chart.data   = [values]
    chart.categoryAxis.categoryNames = labels
    chart.categoryAxis.labels.fontSize  = 8
    chart.categoryAxis.labels.fillColor = GRAY
    chart.categoryAxis.labels.dx        = -4
    chart.valueAxis.labels.fontSize     = 7.5
    chart.valueAxis.labels.fillColor    = GRAY
    def _fmt(v):
        if v >= 1_000_000: return f"${v/1e6:.1f}M"
        if v >= 1_000:     return f"${v/1e3:.0f}K"
        return f"${v:.0f}"
    chart.valueAxis.labelTextFormat = _fmt
    chart.bars[0].fillColor   = bar_colors[0] if bar_colors else BLUE
    chart.bars[0].strokeColor = None
    chart.barWidth = max(8, (chart.height / max(len(labels), 1)) * 0.55)
    d.add(chart)
    return d


def _progress_bar(label, pct, color, width=W):
    bar_w = width - 100
    d = Drawing(width, 22)
    d.add(Rect(90, 6, bar_w, 10, fillColor=GHOST2, strokeColor=BORDER, strokeWidth=0.3))
    fill = bar_w * min(max(float(pct or 0) / 100.0, 0), 1.0)
    if fill > 0:
        d.add(Rect(90, 6, fill, 10, fillColor=color, strokeColor=None))
    d.add(String(0,  8, label[:28],        fontName="Helvetica",      fontSize=7.5, fillColor=GRAY))
    d.add(String(90 + bar_w + 5, 8, f"{float(pct or 0):.1f}%",
                 fontName="Helvetica-Bold", fontSize=7.5, fillColor=color))
    return d


def _motivation_bar(score, width=W):
    d = Drawing(width, 32)
    sw = width / 3.0
    d.add(Rect(0,      14, sw,   14, fillColor=RED_BG,   strokeColor=None))
    d.add(Rect(sw,     14, sw,   14, fillColor=AMBER_BG, strokeColor=None))
    d.add(Rect(sw*2,   14, sw,   14, fillColor=GREEN_BG, strokeColor=None))
    d.add(String(sw*0.5-10, 16, "LOW",    fontName="Helvetica", fontSize=7, fillColor=RED_))
    d.add(String(sw*1.5-14, 16, "MEDIUM", fontName="Helvetica", fontSize=7, fillColor=AMBER))
    d.add(String(sw*2.5-10, 16, "HIGH",   fontName="Helvetica", fontSize=7, fillColor=GREEN))
    nx = (score / 100.0) * width
    d.add(Rect(nx-3, 8, 6, 24, fillColor=DARK, strokeColor=None))
    d.add(String(max(0, nx-10), 0, str(score), fontName="Helvetica-Bold", fontSize=9, fillColor=DARK))
    return d


def _scenario_table(building_sf, assessed, market_low, market_high):
    """Loan scenario table — 3 price points × 2 down payments."""
    prices = [
        ("Ask (Assessed)", assessed or 0),
        ("Market Low",     market_low or 0),
        ("Market High",    market_high or 0),
    ]
    rate_pct   = 7.0
    amort      = 25
    rows = [["Scenario", "Price", "Down 25%", "Loan Amt", "Mo. P&I", "DSCR@7%", "$/SF"]]
    for label, price in prices:
        if not price:
            rows.append([label] + ["N/A"] * 6)
            continue
        for down_pct in (25,):
            down    = price * down_pct / 100
            loan    = price - down
            # Monthly P&I
            r       = rate_pct / 100 / 12
            n       = amort * 12
            mo_pi   = loan * (r * (1+r)**n) / ((1+r)**n - 1) if r > 0 else 0
            # Rough DSCR assuming 7% cap rate on assessed
            noi_est = (assessed or price) * 0.07
            dscr    = (noi_est / 12) / mo_pi if mo_pi > 0 else 0
            psf     = price / building_sf if building_sf and building_sf > 0 else 0
            rows.append([
                label,
                f"${price:,.0f}",
                f"${down:,.0f}",
                f"${loan:,.0f}",
                f"${mo_pi:,.0f}/mo",
                f"{dscr:.2f}x",
                f"${psf:,.0f}/SF" if psf else "N/A",
            ])
    col_w = [1.5*inch, 1.1*inch, 1.0*inch, 1.0*inch, 1.0*inch, 0.8*inch, 0.85*inch]
    t = Table(rows, colWidths=col_w, repeatRows=1)
    t.setStyle(TableStyle([
        ("BACKGROUND",    (0,0),(-1,0),  BLUE_D),
        ("TEXTCOLOR",     (0,0),(-1,0),  WHITE),
        ("FONTNAME",      (0,0),(-1,0),  "Helvetica-Bold"),
        ("FONTSIZE",      (0,0),(-1,0),  7.5),
        ("ROWBACKGROUNDS",(0,1),(-1,-1), [WHITE, GHOST2]),
        ("FONTNAME",      (0,1),(-1,-1), "Helvetica"),
        ("FONTSIZE",      (0,1),(-1,-1), 8),
        ("TEXTCOLOR",     (0,1),(-1,-1), DARK),
        ("GRID",          (0,0),(-1,-1), 0.4, BORDER),
        ("LEFTPADDING",   (0,0),(-1,-1), 5),
        ("TOPPADDING",    (0,0),(-1,-1), 4),
        ("BOTTOMPADDING", (0,0),(-1,-1), 4),
        ("ALIGN",         (1,0),(-1,-1), "RIGHT"),
    ]))
    return t


def _pending_box(title, bullets, width=W):
    """Placeholder section for data that activates with ATTOM."""
    btext = "".join(f"<br/>  •  {b}" for b in bullets)
    inner = Table([[
        _PB(f"📊  {title}", fontSize=10, textColor=BLUE, alignment=TA_CENTER),
        ], [
        _P(f"<i>Activates when ATTOM_API_KEY is configured</i>{btext}",
           fontSize=8.5, textColor=MID, alignment=TA_CENTER, leading=14),
    ]], colWidths=[width - 0.8*inch])
    inner.setStyle(TableStyle([
        ("TOPPADDING",    (0,0),(-1,-1), 6),
        ("BOTTOMPADDING", (0,0),(-1,-1), 6),
        ("LEFTPADDING",   (0,0),(-1,-1), 0),
    ]))
    outer = Table([[inner]], colWidths=[width])
    outer.setStyle(TableStyle([
        ("BACKGROUND",    (0,0),(-1,-1), GHOST),
        ("BOX",           (0,0),(-1,-1), 1, BORDER),
        ("LEFTPADDING",   (0,0),(-1,-1), 20),
        ("TOPPADDING",    (0,0),(-1,-1), 16),
        ("BOTTOMPADDING", (0,0),(-1,-1), 16),
    ]))
    return outer


def _comp_table(comps):
    if not comps:
        return None
    rows = [["Address", "Sold Date", "Sale Price", "Bldg SF", "$/SF", "Beds/Ba", "Yr Built"]]
    for c in comps[:20]:
        rows.append([
            (c.get("address","—") + ", " + c.get("city",""))[:32],
            c.get("sale_date","—")[:10],
            c.get("sale_fmt","—"),
            c.get("sf_fmt") or (f"{int(c['building_sf']):,}" if c.get("building_sf") else "—"),
            c.get("psf_fmt","—"),
            f"{c.get('beds','—')}/{c.get('baths','—')}",
            str(c.get("year_built","—")),
        ])
    col_w = [2.3*inch, 0.85*inch, 0.9*inch, 0.75*inch, 0.7*inch, 0.65*inch, 0.65*inch]
    t = Table(rows, colWidths=col_w, repeatRows=1)
    t.setStyle(TableStyle([
        ("BACKGROUND",    (0,0),(-1,0),  BLUE),
        ("TEXTCOLOR",     (0,0),(-1,0),  WHITE),
        ("FONTNAME",      (0,0),(-1,0),  "Helvetica-Bold"),
        ("FONTSIZE",      (0,0),(-1,0),  7),
        ("ROWBACKGROUNDS",(0,1),(-1,-1), [WHITE, GHOST2]),
        ("FONTNAME",      (0,1),(-1,-1), "Helvetica"),
        ("FONTSIZE",      (0,1),(-1,-1), 7.5),
        ("GRID",          (0,0),(-1,-1), 0.4, BORDER),
        ("LEFTPADDING",   (0,0),(-1,-1), 4),
        ("TOPPADDING",    (0,0),(-1,-1), 4),
        ("BOTTOMPADDING", (0,0),(-1,-1), 4),
        ("FONTNAME",      (2,1),(2,-1),  "Helvetica-Bold"),
        ("TEXTCOLOR",     (2,1),(2,-1),  BLUE),
    ]))
    return t


# ════════════════════════════════════════════════════════════════════════════
# MAIN GENERATOR
# ════════════════════════════════════════════════════════════════════════════

def generate_pdf_bytes(report: dict) -> bytes:
    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=letter,
                            rightMargin=RM, leftMargin=LM,
                            topMargin=TM,   bottomMargin=BM,
                            title="PropIntel Report")

    e = []  # elements

    # ── shortcuts ────────────────────────────────────────────────────────────
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
    comps_d = report.get("sold_comps", {}) or {}
    mort    = report.get("mortgage", {}) or {}
    hist    = report.get("ownership_history", {}) or {}
    permit_ = report.get("permit_portal", {}) or {}
    ws      = report.get("walkscore", {}) or {}
    liens   = report.get("liens", {}) or {}
    tier    = report.get("tier", "starter")
    is_pro  = tier == "pro"
    raw_addr = (p.get("property_address") or report.get("input","Unknown"))
    county_str = f"{p.get('county','').title()} County, {p.get('state','TX')}" if p.get("county") else p.get("state","TX")
    full_addr = f"{raw_addr}, {county_str}"
    gen  = (report.get("generated_at") or datetime.utcnow().isoformat())[:10]
    assessed  = float(p.get("assessed_total") or 0)
    land_val  = float(p.get("assessed_land") or 0)
    impr_val  = float(p.get("assessed_improvement") or 0)
    mkt_low   = float(mkt.get("market_low") or 0)
    mkt_high  = float(mkt.get("market_high") or 0)
    mkt_mid   = (mkt_low + mkt_high) / 2 if mkt_low and mkt_high else assessed * 1.05
    bldg_sf   = float(p.get("building_sf") or 0)

    # ════════════════════════════════════════════════════════════════════════
    # PAGE 1 — EXECUTIVE SUMMARY
    # ════════════════════════════════════════════════════════════════════════

    # Brand bar
    brand_row = Table([[
        _PB("PropIntel", fontSize=22, textColor=DARK, leading=26),
        _P(('PRO' if is_pro else 'STARTER') + f" Report  ·  {gen}",
           fontSize=9, textColor=GRAY, alignment=TA_RIGHT, leading=12),
    ]], colWidths=[W*0.55, W*0.45])
    brand_row.setStyle(TableStyle([
        ("VALIGN", (0,0),(-1,-1), "BOTTOM"),
        ("LEFTPADDING",  (0,0),(-1,-1), 0),
        ("RIGHTPADDING", (0,0),(-1,-1), 0),
    ]))
    e.append(brand_row)
    e.append(_hr(BLUE, 2.5, 4, 8))

    # Address block
    addr_tbl = Table([[_PB(full_addr, fontSize=13, textColor=DARK, leading=16)]],
                     colWidths=[W])
    addr_tbl.setStyle(TableStyle([
        ("BACKGROUND",    (0,0),(-1,-1), GHOST),
        ("BOX",           (0,0),(-1,-1), 0.5, BORDER),
        ("LEFTPADDING",   (0,0),(-1,-1), 12),
        ("TOPPADDING",    (0,0),(-1,-1), 10),
        ("BOTTOMPADDING", (0,0),(-1,-1), 10),
    ]))
    e.append(addr_tbl)

    # Badge pills
    badges = []
    if p.get("absentee_owner"):   badges.append(("ABSENTEE OWNER", AMBER_BG,   AMBER))
    if p.get("out_of_state_owner"):badges.append(("OUT OF STATE",   RED_BG,    RED_))
    if ent.get("entity_name") and not ent.get("is_individual"):
                                   badges.append(("ENTITY OWNER",  BLUE_LIGHT, BLUE))
    if p.get("tax_delinquent"):    badges.append(("TAX DELINQUENT",RED_BG,    RED_))
    if fld.get("zone") and "AE" in (fld.get("zone","").upper()):
                                   badges.append(("FLOOD ZONE AE", RED_BG,    RED_))
    br = _badge_row(badges)
    if br:
        e.append(br)
    e.append(Spacer(1, 10))

    # 4-up Key Metrics
    assessed_fmt = f"${assessed/1e3:.0f}K" if assessed < 1e6 else f"${assessed/1e6:.2f}M" if assessed else "N/A"
    mkt_fmt   = mkt.get("range_fmt") or ("Pending" if not avm.get("available") else avm.get("range_fmt","N/A"))
    sf_fmt    = f"{int(bldg_sf):,} SF" if bldg_sf else "N/A"
    flood_z   = fld.get("zone","N/A")
    e.append(_tile4([
        (assessed_fmt,  "COUNTY ASSESSED",   "Tax value — not market",     BLUE),
        (mkt_fmt,       "EST. MARKET RANGE",  mkt.get("confidence","DFW avg"), TEAL),
        (sf_fmt,        "BUILDING SIZE",      f"Built {p.get('year_built','N/A')}", PURPLE),
        (flood_z,       "FEMA FLOOD ZONE",    "Minimal" if "X" in flood_z else "Verify insurance",
                        GREEN if "X" in flood_z else RED_),
    ], {}))
    e.append(Spacer(1, 6))

    # Row 2 tiles
    score_val = mot.get("score", 0)
    score_col = GREEN if mot.get("tier") == "HIGH" else (AMBER if mot.get("tier") == "MEDIUM" else RED_)
    use_str   = (p.get("use_description") or "N/A")[:18]
    zoning    = p.get("zoning","N/A")
    tax_ann   = fin.get("est_annual_tax")
    tax_fmt   = f"${tax_ann:,.0f}/yr" if tax_ann else "N/A"
    e.append(_tile4([
        (f"{score_val}/100", "MOTIVATION SCORE", mot.get("tier","N/A"), score_col),
        (use_str,           "USE TYPE",          zoning,                SLATE),
        (tax_fmt,           "EST. ANNUAL TAX",   f"{fin.get('tax_rate_pct','N/A')}% eff. rate", ORANGE),
        (f"{int(p.get('year_built',0))}" if p.get("year_built") else "N/A",
                            "YEAR BUILT",        f"APN: {p.get('apn','N/A')}", BLUE),
    ], {}))
    e.append(Spacer(1, 12))

    # Investment Signals
    if flgs:
        e.append(_sec("Investment Signals"))
        sig_data = []
        for f_ in flgs:
            t_ = f_.get("type","green")
            icon  = "✓" if t_=="green" else ("⚠" if t_=="yellow" else "✗")
            clr   = GREEN if t_=="green" else (AMBER if t_=="yellow" else RED_)
            bg_   = GREEN_BG if t_=="green" else (AMBER_BG if t_=="yellow" else RED_BG)
            sig_data.append([
                _PB(icon, fontSize=10, textColor=clr, alignment=TA_CENTER),
                _P(f_.get("text",""), fontSize=9, textColor=clr, leading=12),
            ])
        st = Table(sig_data, colWidths=[0.35*inch, W - 0.4*inch])
        st.setStyle(TableStyle([
            ("ROWBACKGROUNDS", (0,0),(-1,-1), [WHITE, GHOST2]),
            ("GRID",           (0,0),(-1,-1), 0.4, BORDER),
            ("LEFTPADDING",    (0,0),(-1,-1), 8),
            ("TOPPADDING",     (0,0),(-1,-1), 6),
            ("BOTTOMPADDING",  (0,0),(-1,-1), 6),
            ("VALIGN",         (0,0),(-1,-1), "MIDDLE"),
        ]))
        e.append(st)

    # ════════════════════════════════════════════════════════════════════════
    # PAGE 2 — PARCEL & PROPERTY DETAILS (ALL FIELDS)
    # ════════════════════════════════════════════════════════════════════════
    e.append(PageBreak())
    e.append(_h2("Parcel & Property Details"))
    e.append(_hr(BLUE, 1.5, 2, 8))

    e.append(_sec("Ownership"))
    own_rows = [
        ("Owner of Record",          p.get("owner_name")),
        ("Secondary Owner / Co-Owner",p.get("owner_name2")),
        ("Owner Type",               "Entity / Corporation" if (ent.get("entity_name") and not ent.get("is_individual")) else "Individual"),
        ("Owner Mailing Address",    p.get("owner_mailing")),
        ("Owner City",               p.get("owner_city")),
        ("Owner State",              p.get("owner_state")),
        ("Owner ZIP",                p.get("owner_zip")),
        ("Absentee Owner",           "YES — mailing ≠ property address" if p.get("absentee_owner") else "No"),
        ("Out-of-State Owner",       "YES" if p.get("out_of_state_owner") else "No"),
    ]
    t = _kv([(l,v) for l,v in own_rows])
    if t: e.append(t)

    e.append(_sec("Property Identification"))
    id_rows = [
        ("APN / Parcel Number",   p.get("apn")),
        ("Regrid UUID",           p.get("regrid_uuid")),
        ("Property Address",      p.get("property_address")),
        ("County",                (p.get("county") or "").title()),
        ("State",                 p.get("state")),
        ("Data Source",           p.get("data_sources") or p.get("source")),
    ]
    t = _kv([(l,v) for l,v in id_rows])
    if t: e.append(t)

    e.append(_sec("Property Characteristics"))
    char_rows = [
        ("Use Description",       p.get("use_description")),
        ("Property Class",        p.get("property_class")),
        ("Zoning",                p.get("zoning")),
        ("Building Size (SF)",    f"{int(bldg_sf):,} SF" if bldg_sf else None),
        ("Lot Size (Acres)",      p.get("lot_acres")),
        ("Year Built",            p.get("year_built")),
        ("Tax Delinquent",        "YES — contact county" if p.get("tax_delinquent") else "No"),
    ]
    t = _kv([(l,v) for l,v in char_rows])
    if t: e.append(t)

    e.append(_sec("Tax Assessment"))
    tax_rows = [
        ("Assessed Total (Tax Value)",  f"${assessed:,.0f}" if assessed else None),
        ("Assessed Land Value",         f"${land_val:,.0f}" if land_val else None),
        ("Assessed Improvement Value",  f"${impr_val:,.0f}" if impr_val else None),
        ("Previous Year Assessed",      f"${p.get('assessed_prev',0):,.0f}" if p.get("assessed_prev") else None),
        ("Year-over-Year Change",       f"{p.get('assessed_yoy_pct')}%" if p.get("assessed_yoy_pct") is not None else "Not available"),
        ("Tax Delinquency Status",      "Current — no delinquency found" if not p.get("tax_delinquent") else "DELINQUENT"),
    ]
    t = _kv([(l,v) for l,v in tax_rows])
    if t: e.append(t)

    # Bar chart: assessed breakdown
    if land_val or impr_val:
        e.append(_sec("Assessment Breakdown"))
        chart = _hbar_chart(
            ["Market Est.", "Assessed Total", "Improvements", "Land Value"],
            [mkt_mid, assessed, impr_val, land_val],
            [TEAL, BLUE, PURPLE, ORANGE], W, 2.0*inch
        )
        if chart: e.append(chart)

    # ════════════════════════════════════════════════════════════════════════
    # PAGE 3 — VALUATION & MARKET ANALYSIS
    # ════════════════════════════════════════════════════════════════════════
    e.append(PageBreak())
    e.append(_h2("Valuation & Market Analysis"))
    e.append(_hr(BLUE, 1.5, 2, 8))

    e.append(_sec("County Tax Assessment vs. Market Value"))
    val_rows = [
        ("County Assessed (Tax Value)", f"${assessed:,.0f}" if assessed else "N/A"),
        ("  ↳ Assessed Land",           f"${land_val:,.0f}" if land_val else "N/A"),
        ("  ↳ Assessed Improvements",   f"${impr_val:,.0f}" if impr_val else "N/A"),
        ("Land as % of Total",          f"{land_val/assessed*100:.1f}%" if assessed and land_val else "N/A"),
        ("Improvement as % of Total",   f"{impr_val/assessed*100:.1f}%" if assessed and impr_val else "N/A"),
        ("Est. Market Range",           mkt.get("range_fmt","N/A")),
        ("Market Estimate Method",      mkt.get("methodology") or mkt.get("note","N/A")),
        ("Confidence Level",            mkt.get("confidence","N/A")),
        ("Assessed vs. Market Mid",     f"${mkt_mid - assessed:+,.0f} ({(mkt_mid/assessed - 1)*100:+.1f}%)" if assessed and mkt_mid else "N/A"),
        ("Property Type (for AVM)",     mkt.get("property_type","N/A")),
    ]
    t = _kv(val_rows)
    if t: e.append(t)

    e.append(_sec("ATTOM Automated Valuation Model (AVM)"))
    if avm.get("available"):
        avm_rows = [
            ("AVM Value",           avm.get("value_fmt")),
            ("AVM Range (Low)",     f"${avm.get('value_low',0):,.0f}" if avm.get("value_low") else None),
            ("AVM Range (High)",    f"${avm.get('value_high',0):,.0f}" if avm.get("value_high") else None),
            ("AVM Range (Full)",    avm.get("range_fmt")),
            ("Confidence Score",    f"{avm.get('confidence_score')}%" if avm.get("confidence_score") else None),
            ("AVM Calculation Date",avm.get("calc_date")),
            ("Source",              "ATTOM Data Solutions"),
        ]
        t = _kv([(l,v) for l,v in avm_rows if v is not None])
        if t: e.append(t)
    else:
        e.append(_pending_box("ATTOM AVM", [
            "Automated Valuation Model (AVM) with confidence score",
            "Value range (low / mid / high)",
            "AVM calculation date and methodology",
            "Replaces the assessed-based estimate above with real market model",
        ]))

    e.append(_sec("Equity & Leverage Analysis"))
    if mort.get("available"):
        equity = mkt_mid - (mort.get("open_lien_balance") or mort.get("loan_amount") or 0)
        ltv    = ((mort.get("open_lien_balance") or mort.get("loan_amount") or 0) / mkt_mid * 100) if mkt_mid else 0
        eq_rows = [
            ("Est. Market Value (Mid)",  f"${mkt_mid:,.0f}"),
            ("Open Lien Balance",        mort.get("open_lien_fmt","N/A")),
            ("Estimated Equity",         f"${equity:,.0f}"),
            ("Estimated LTV",            f"{ltv:.1f}%"),
            ("Equity Ratio",             f"{(equity/mkt_mid*100):.1f}%" if mkt_mid and equity > 0 else "N/A"),
        ]
        t = _kv(eq_rows)
        if t: e.append(t)
    else:
        e.append(_pending_box("Mortgage & Equity Analysis", [
            "Open loan balance and estimated equity",
            "Loan-to-value (LTV) ratio",
            "Lender name and loan type",
            "Refinance history",
        ]))

    # Investment scenario table
    e.append(_sec("Purchase Scenario Modeling"))
    e.append(_P("Estimates only — verify with actual financials and lender. Uses 7% rate, 25yr amort, 25% down.",
                fontSize=7.5, textColor=GRAY, leading=10))
    e.append(Spacer(1,4))
    e.append(_scenario_table(bldg_sf, assessed, mkt_low, mkt_high))

    # ════════════════════════════════════════════════════════════════════════
    # PAGE 4 — FINANCIAL ANALYSIS
    # ════════════════════════════════════════════════════════════════════════
    e.append(PageBreak())
    e.append(_h2("Financial Analysis"))
    e.append(_hr(BLUE, 1.5, 2, 8))

    if fin.get("available"):
        e.append(_sec("Tax Estimates"))
        e.append(_tile4([
            (f"${fin.get('est_annual_tax',0):,.0f}", "EST. ANNUAL TAX",  f"{fin.get('tax_rate_pct',0)}% eff. rate", BLUE),
            (f"${fin.get('est_monthly_tax',0):,.0f}","EST. MONTHLY TAX", "Budget line item",                        TEAL),
            ("Dallas Co.", "TAX DISTRICT",           "DISD + county + city",                                        PURPLE),
            ("2025",       "TAX YEAR",               "Based on current assessment",                                  SLATE),
        ], {}))
        e.append(Spacer(1,6))
        e.append(_kv([
            ("Effective Tax Rate",  f"{fin.get('tax_rate_pct',0)}%"),
            ("Est. Annual Tax",     f"${fin.get('est_annual_tax',0):,.0f}"),
            ("Est. Monthly Tax",    f"${fin.get('est_monthly_tax',0):,.0f}"),
            ("Tax Note",            fin.get("tax_note")),
        ]))

        if fin.get("cash_flow"):
            e.append(_sec("Commercial Income Estimates"))
            e.append(_tile4([
                (fin.get("gsi_range","N/A"),      "GROSS INCOME (GSI)",  "Annual est.",         PURPLE),
                (fin.get("noi_range","N/A"),      "NET OPER. INCOME",    "65% expense ratio",   GREEN),
                (f"{fin.get('implied_cap_rate','N/A')}%", "IMPLIED CAP RATE", "At market mid",  TEAL),
                (fin.get("rent_per_sf_range","N/A"), "MARKET RENT",       f"/SF/yr — {fin.get('rent_use_label','')}", ORANGE),
            ], {}))
            e.append(Spacer(1,6))
            e.append(_kv([
                ("Building SF",          f"{int(fin.get('building_sf',0)):,} SF" if fin.get("building_sf") else "N/A"),
                ("Use Type",             fin.get("rent_use_label")),
                ("Market Rent Range",    fin.get("rent_per_sf_range")),
                ("Gross Scheduled Income (GSI)", fin.get("gsi_range")),
                ("Net Operating Income (NOI)",   fin.get("noi_range")),
                ("Expense Ratio",        "65% (DFW commercial average)"),
                ("Implied Cap Rate",     f"{fin.get('implied_cap_rate','N/A')}%"),
                ("Cap Rate Note",        "Based on est. market mid-value — verify with rent roll"),
                ("Cash Flow Note",       fin.get("cash_flow_note")),
            ]))
    else:
        e.append(_P("Financial estimates not available for this property type.", fontSize=9, textColor=GRAY))

    e.append(_sec("Deal Analysis (Stated Listing Terms)"))
    loan = da.get("loan_assumptions") or {}
    da_rows = [
        ("Asking Price",                    da.get("asking_price_fmt") or "Not listed"),
        ("Price per SF",                    f"${da.get('price_per_sf'):,.0f}/SF" if da.get("price_per_sf") else "N/A"),
        ("Building SF",                     f"{int(da.get('building_sf',0)):,} SF" if da.get("building_sf") else "N/A"),
        ("Stated Cap Rate",                 f"{da.get('stated_cap_rate')}%" if da.get("stated_cap_rate") else "Not provided"),
        ("Stated NOI",                      da.get("stated_noi_fmt") or "Not provided"),
        ("Assessed vs. Asking Premium",     f"+{da.get('assessed_vs_asking_premium_pct')}% over assessed" if da.get("assessed_vs_asking_premium_pct") else "N/A"),
        ("Debt Service Coverage (DSCR)",    f"{da.get('dscr'):.2f}x" if da.get("dscr") else "N/A — no asking price"),
        ("Cash-on-Cash Return (Est.)",      f"{da.get('cash_on_cash_pct')}%" if da.get("cash_on_cash_pct") else "N/A"),
        ("Monthly Debt Service",            f"${da.get('monthly_debt_service'):,.0f}/mo" if da.get("monthly_debt_service") else "N/A"),
        ("Assumed LTV",                     f"{loan.get('ltv_pct',75)}%"),
        ("Assumed Interest Rate",           f"{loan.get('rate_pct',7.0)}%"),
        ("Assumed Amortization",            f"{loan.get('amortization_years',25)} years"),
        ("Deal Analyzer Note",              da.get("note")),
    ]
    t = _kv([(l,v) for l,v in da_rows if v is not None])
    if t: e.append(t)

    # ════════════════════════════════════════════════════════════════════════
    # PAGE 5 — SELLER MOTIVATION
    # ════════════════════════════════════════════════════════════════════════
    e.append(PageBreak())
    e.append(_h2("Seller Motivation Analysis"))
    e.append(_hr(BLUE, 1.5, 2, 8))

    score     = mot.get("score", 0)
    mot_tier  = mot.get("tier","LOW")
    s_color   = GREEN if mot_tier=="HIGH" else (AMBER if mot_tier=="MEDIUM" else RED_)
    s_bg      = GREEN_BG if mot_tier=="HIGH" else (AMBER_BG if mot_tier=="MEDIUM" else RED_BG)

    # Score display
    score_tbl = Table([[
        _PB(str(score), fontSize=40, textColor=s_color, alignment=TA_CENTER, leading=44),
        Table([
            [_PB(mot_tier, fontSize=14, textColor=s_color, leading=17)],
            [_P(mot.get("interpretation",""), fontSize=8.5, textColor=MID, leading=13)],
            [_P(f"Score methodology: {mot.get('note','Public records only')}", fontSize=7, textColor=GRAY, leading=10)],
        ], colWidths=[W - 1.3*inch]),
    ]], colWidths=[1.2*inch, W - 1.3*inch])
    score_tbl.setStyle(TableStyle([
        ("BACKGROUND",    (0,0),(-1,-1), s_bg),
        ("BOX",           (0,0),(-1,-1), 1.5, s_color),
        ("LEFTPADDING",   (0,0),(-1,-1), 12),
        ("TOPPADDING",    (0,0),(-1,-1), 14),
        ("BOTTOMPADDING", (0,0),(-1,-1), 14),
        ("VALIGN",        (0,0),(-1,-1), "MIDDLE"),
    ]))
    e.append(score_tbl)
    e.append(Spacer(1, 8))
    e.append(_motivation_bar(score, W))
    e.append(Spacer(1, 12))

    # All indicators table
    indicators = mot.get("indicators", [])
    if indicators:
        e.append(_sec("All Motivation Indicators"))
        ind_rows = [["Signal", "Status", "Points", "Evidence", "Source"]]
        for ind in indicators:
            triggered = "✓  TRIGGERED" if ind.get("triggered") else "—  Not triggered"
            t_color   = GREEN if ind.get("triggered") else GRAY
            pts       = f"+{ind['points']}" if ind.get("triggered") else "0"
            ind_rows.append([
                ind.get("name","")[:28],
                triggered,
                pts,
                ind.get("evidence","")[:40],
                ind.get("source","")[:35],
            ])
        ind_t = Table(ind_rows, colWidths=[1.8*inch, 1.1*inch, 0.5*inch, 2.1*inch, 1.75*inch],
                      repeatRows=1)
        ind_t.setStyle(TableStyle([
            ("BACKGROUND",    (0,0),(-1,0),  MID),
            ("TEXTCOLOR",     (0,0),(-1,0),  WHITE),
            ("FONTNAME",      (0,0),(-1,0),  "Helvetica-Bold"),
            ("FONTSIZE",      (0,0),(-1,0),  7),
            ("ROWBACKGROUNDS",(0,1),(-1,-1), [WHITE, GHOST2]),
            ("FONTNAME",      (0,1),(-1,-1), "Helvetica"),
            ("FONTSIZE",      (0,1),(-1,-1), 7.5),
            ("TEXTCOLOR",     (0,1),(-1,-1), DARK),
            ("GRID",          (0,0),(-1,-1), 0.4, BORDER),
            ("LEFTPADDING",   (0,0),(-1,-1), 5),
            ("TOPPADDING",    (0,0),(-1,-1), 4),
            ("BOTTOMPADDING", (0,0),(-1,-1), 4),
            ("VALIGN",        (0,0),(-1,-1), "MIDDLE"),
        ]))
        e.append(ind_t)

    # ════════════════════════════════════════════════════════════════════════
    # PAGE 6 — OWNER INTELLIGENCE
    # ════════════════════════════════════════════════════════════════════════
    e.append(PageBreak())
    e.append(_h2("Owner Intelligence"))
    e.append(_hr(BLUE, 1.5, 2, 8))

    e.append(_sec("Owner Entity (Texas Secretary of State)"))
    entity_name = ent.get("entity_name","")
    if entity_name and not ent.get("is_individual"):
        ent_err = ent.get("error","")
        if ent.get("status") or ent.get("formation_date"):
            ent_rows = [
                ("Entity Name",       entity_name),
                ("TX SOS Status",     ent.get("status")),
                ("Formation Date",    ent.get("formation_date")),
                ("Registered Agent",  ent.get("registered_agent")),
                ("TX SOS Record URL", ent.get("manual_url")),
                ("Forfeiture Risk",   "Check TX SOS status above"),
            ]
            t = _kv([(l,v) for l,v in ent_rows if v not in (None,"")])
            if t: e.append(t)
        else:
            e.append(_kv([
                ("Entity Name",    entity_name),
                ("TX SOS Status",  "Lookup blocked (bot protection) — search manually"),
                ("Manual Search",  ent.get("manual_url","https://www.sos.state.tx.us")),
            ]))
        if ent_err and "403" in ent_err:
            e.append(Spacer(1,4))
            e.append(_P("⚠  TX SOS website blocked automated lookup. Visit the link above and search by entity name directly.",
                        fontSize=8, textColor=AMBER, leading=12))
    else:
        e.append(_kv([
            ("Owner",       p.get("owner_name","Unknown")),
            ("Owner Type",  "Individual (not an entity)"),
        ]))

    e.append(_sec("Skip Trace / Owner Contact" + (" — Pro Plan" if is_pro else " — Starter (Upgrade for Contact)")))
    if is_pro:
        sk_status = sk.get("status","")
        e.append(_kv([
            ("Status",          sk_status),
            ("Source",          sk.get("source")),
            ("Credits Used",    str(sk.get("credits_used",0))),
        ]))
        if sk_status == "hit":
            phones = sk.get("phones",[])
            emails_ = sk.get("emails",[])
            e.append(Spacer(1,5))
            if phones:
                e.append(_kv([("Phone Numbers", ", ".join(phones[:6]))]))
            if emails_:
                e.append(_kv([("Email Addresses", ", ".join(emails_[:6]))]))
        elif sk.get("note"):
            e.append(Spacer(1,4))
            e.append(_P(sk.get("note",""), fontSize=8.5, textColor=MID, leading=13))
            if sk_status == "entity":
                e.append(Spacer(1,4))
                e.append(_P("For entity owners: contact the registered agent listed in TX SOS above, "
                            "or call the property management company associated with the entity.",
                            fontSize=8, textColor=GRAY, leading=12))
    else:
        e.append(_P("Owner phone numbers and email addresses available on the Pro plan ($29.99).", fontSize=9, textColor=GRAY))

    e.append(_sec("Owner Portfolio (ATTOM)"))
    e.append(_pending_box("Owner Portfolio Intelligence", [
        "How many properties does this owner hold?",
        "Total portfolio equity and debt load",
        "Portfolio purchase history and hold strategy",
        "Signs of distress (over-leveraged portfolio, multiple delinquencies)",
    ]))

    # ════════════════════════════════════════════════════════════════════════
    # PAGE 7 — MORTGAGE, LIENS & TITLE
    # ════════════════════════════════════════════════════════════════════════
    e.append(PageBreak())
    e.append(_h2("Mortgage, Liens & Title"))
    e.append(_hr(BLUE, 1.5, 2, 8))

    e.append(_sec("Mortgage & Open Lien Data (ATTOM)"))
    if mort.get("available"):
        e.append(_kv([
            ("Loan Amount (Original)",   mort.get("loan_amount_fmt")),
            ("Loan Type",                mort.get("loan_type")),
            ("Interest Rate",            f"{mort.get('interest_rate')}%" if mort.get("interest_rate") else None),
            ("Lender Name",              mort.get("lender_name")),
            ("Maturity Date",            mort.get("maturity_date")),
            ("Open Lien Count",          str(mort.get("open_lien_count")) if mort.get("open_lien_count") is not None else None),
            ("Open Lien Total Balance",  mort.get("open_lien_fmt")),
            ("Last Sale Price",          mort.get("last_sale_price_fmt")),
            ("Last Sale Date",           mort.get("last_sale_date")),
        ]))
    else:
        e.append(_pending_box("Mortgage & Lien Data", [
            "Open loan balance and original loan amount",
            "Lender name, loan type, interest rate",
            "Maturity date and refinance history",
            "Open lien count and total lien balance",
            "Last sale price and transaction date",
        ]))

    e.append(_sec("Lien Search"))
    if liens.get("status") or liens.get("note"):
        e.append(_kv([
            ("Lien Status",    liens.get("status")),
            ("Note",           liens.get("note")),
            ("Manual Search",  liens.get("manual_url")),
            ("APN",            liens.get("apn")),
        ]))
    else:
        e.append(_kv([
            ("Lien Status",  "Not available — verify with title company before closing"),
            ("Manual Search","https://dallascounty.org/departments/records/"),
            ("APN",          p.get("apn")),
        ]))
    e.append(Spacer(1,4))
    e.append(_P("⚠  Always order a full title search before closing. Liens, judgments, and encumbrances may not be "
                "visible in public records without a licensed title search.",
                fontSize=8, textColor=AMBER, leading=12))

    # ════════════════════════════════════════════════════════════════════════
    # PAGE 8 — SOLD COMPARABLE SALES
    # ════════════════════════════════════════════════════════════════════════
    e.append(PageBreak())
    e.append(_h2("Sold Comparable Sales"))
    e.append(_hr(BLUE, 1.5, 2, 8))

    comps = comps_d.get("comps",[])
    comp_stats = comps_d.get("stats",{})

    if comps:
        e.append(_tile4([
            (str(comp_stats.get("comp_count",0)),       "COMPS FOUND",    f"{comps_d.get('radius_miles',0.5)}mi radius", BLUE),
            (comp_stats.get("median_price_fmt","N/A"),  "MEDIAN PRICE",   f"Last {comps_d.get('months_back',12)} months", TEAL),
            (comp_stats.get("median_psf_fmt","N/A"),    "MEDIAN $/SF",    "All comps",         PURPLE),
            (comp_stats.get("price_range_fmt","N/A"),   "PRICE RANGE",    "Low → High",        ORANGE),
        ], {}))
        e.append(Spacer(1,8))
        ct = _comp_table(comps)
        if ct: e.append(ct)
        e.append(Spacer(1,5))
        e.append(_P(f"Source: ATTOM Data Solutions  ·  {comp_stats.get('comp_count',0)} sales within "
                    f"{comps_d.get('radius_miles',0.5)} miles  ·  Last {comps_d.get('months_back',12)} months",
                    fontSize=7.5, textColor=GRAY))
    else:
        e.append(_pending_box("Sold Comparable Sales", [
            "Up to 15-20 sold comps within 0.5 miles",
            "90-day, 6-month, and 12-month breakdowns selectable",
            "Sale price, price/SF, beds/baths, year built, days on market",
            "Median and average comp statistics with price range",
            "Price trend over time (is the market rising or falling?)",
        ]))

    e.append(_sec("Comparable Sales — Research Links"))
    enc = urllib.parse.quote(raw_addr + " " + (p.get("county","") + " TX").strip())
    e.append(_kv([
        ("Zillow Recently Sold",  f"https://www.zillow.com/homes/recently_sold/{enc}_rb/"),
        ("Redfin Sold",           f"https://www.redfin.com/TX"),
        ("LoopNet Sold",          "https://www.loopnet.com/search/commercial-real-estate/dallas-county-tx/sold/"),
        ("DCAD Sales Search",     "https://www.dallascad.org/AcctDetailRes.aspx"),
    ]))

    # ════════════════════════════════════════════════════════════════════════
    # PAGE 9 — OWNERSHIP HISTORY & DEED RECORDS
    # ════════════════════════════════════════════════════════════════════════
    e.append(PageBreak())
    e.append(_h2("Ownership History & Deed Records"))
    e.append(_hr(BLUE, 1.5, 2, 8))

    history = hist.get("history",[])
    if history:
        if hist.get("hold_years"):
            e.append(_P(f"Current owner has held this property for approximately <b>{hist['hold_years']} years</b>. "
                        f"Long hold duration is often a motivation indicator.",
                        fontSize=9.5, textColor=DARK, leading=14))
            e.append(Spacer(1,6))
        hist_rows = [["Sale Date","Buyer","Sale Price","Document Type"]]
        for h_ in history:
            hist_rows.append([
                h_.get("sale_date","—")[:10],
                (h_.get("buyer_name") or "—")[:35],
                h_.get("sale_fmt") or "—",
                (h_.get("document_type") or "—")[:28],
            ])
        ht = Table(hist_rows, colWidths=[1.0*inch, 2.7*inch, 1.2*inch, 2.3*inch], repeatRows=1)
        ht.setStyle(TableStyle([
            ("BACKGROUND",    (0,0),(-1,0),  MID),
            ("TEXTCOLOR",     (0,0),(-1,0),  WHITE),
            ("FONTNAME",      (0,0),(-1,0),  "Helvetica-Bold"),
            ("FONTSIZE",      (0,0),(-1,0),  7.5),
            ("ROWBACKGROUNDS",(0,1),(-1,-1), [WHITE, GHOST2]),
            ("FONTNAME",      (0,1),(-1,-1), "Helvetica"),
            ("FONTSIZE",      (0,1),(-1,-1), 8),
            ("GRID",          (0,0),(-1,-1), 0.4, BORDER),
            ("LEFTPADDING",   (0,0),(-1,-1), 6),
            ("TOPPADDING",    (0,0),(-1,-1), 4),
            ("BOTTOMPADDING", (0,0),(-1,-1), 4),
        ]))
        e.append(ht)
    else:
        e.append(_pending_box("Deed & Ownership History", [
            "Full transaction history — every sale going back 20+ years",
            "Buyer and seller names on each transaction",
            "Purchase prices and recording dates",
            "Hold duration calculation (how long current owner has held)",
            "Document types (deed, transfer, foreclosure, etc.)",
        ]))

    e.append(_sec("Deed Records — Manual Research Links"))
    e.append(_kv([
        ("Dallas County Deed Search",  "https://www.dallascounty.org/departments/records/"),
        ("DCAD Property Search",       "https://www.dallascad.org"),
        ("TX Land Records (statewide)","https://www.txlandrecords.com"),
    ]))

    # ════════════════════════════════════════════════════════════════════════
    # PAGE 10 — FEMA FLOOD & ENVIRONMENTAL
    # ════════════════════════════════════════════════════════════════════════
    e.append(PageBreak())
    e.append(_h2("FEMA Flood Zone & Environmental"))
    e.append(_hr(BLUE, 1.5, 2, 8))

    zone      = fld.get("zone","N/A")
    is_x      = "X" in zone.upper()
    is_ae     = "AE" in zone.upper()
    fld_color = GREEN if is_x else RED_
    fld_bg    = GREEN_BG if is_x else RED_BG

    flood_banner = Table([[
        _PB(zone, fontSize=32, textColor=fld_color, alignment=TA_CENTER, leading=36),
        Table([
            [_PB(fld.get("description","No description available"),
                 fontSize=12, textColor=fld_color, leading=15)],
            [_P("No flood insurance required (conventional financing OK)." if is_x
                else "⚠ Flood insurance likely REQUIRED. Contact lender.",
                fontSize=9, textColor=MID, leading=12)],
            [_P(f"FIRM Panel: {fld.get('firm_panel','N/A')}  ·  Insurance Required: "
                f"{'Yes' if fld.get('flood_insurance_required') else ('Likely Yes' if is_ae else 'No')}",
                fontSize=8, textColor=GRAY, leading=11)],
        ], colWidths=[W - 1.5*inch]),
    ]], colWidths=[1.4*inch, W - 1.5*inch])
    flood_banner.setStyle(TableStyle([
        ("BACKGROUND",    (0,0),(-1,-1), fld_bg),
        ("BOX",           (0,0),(-1,-1), 1.5, fld_color),
        ("LEFTPADDING",   (0,0),(-1,-1), 12),
        ("TOPPADDING",    (0,0),(-1,-1), 14),
        ("BOTTOMPADDING", (0,0),(-1,-1), 14),
        ("VALIGN",        (0,0),(-1,-1), "MIDDLE"),
    ]))
    e.append(flood_banner)

    e.append(_sec("Flood Zone Detail"))
    e.append(_kv([
        ("Zone Designation",          fld.get("zone")),
        ("Zone Description",          fld.get("description")),
        ("FIRM Panel Number",         fld.get("firm_panel")),
        ("Flood Insurance Required",  "Yes" if fld.get("flood_insurance_required") else ("Likely — AE zone" if is_ae else "No")),
        ("Source",                    fld.get("source","FEMA National Flood Hazard Layer (NFHL)")),
        ("Verify at",                 fld.get("source_url","https://msc.fema.gov/portal/home")),
    ]))

    e.append(_sec("What This Means"))
    if is_x:
        e.append(_P(
            "Zone X is the lowest-risk flood designation. No flood insurance is required by lenders for conventional financing. "
            "The property is outside the 100-year and 500-year floodplains. This is a positive factor for financing, insurance costs, "
            "and tenant/buyer appeal.",
            fontSize=9, textColor=MID, leading=14))
    elif is_ae:
        e.append(_P(
            "Zone AE is a high-risk Special Flood Hazard Area (SFHA). Flood insurance IS required for federally backed mortgages. "
            "Annual flood insurance premiums can range from $800–$10,000+ depending on property elevation and flood risk. "
            "Request an Elevation Certificate before closing and price this into your underwriting.",
            fontSize=9, textColor=MID, leading=14))
    else:
        e.append(_P(
            "Verify flood risk details at FEMA's Flood Map Service Center: msc.fema.gov. "
            "Always confirm flood zone with lender and insurance agent before closing.",
            fontSize=9, textColor=MID, leading=14))

    e.append(_sec("Environmental Research Links"))
    e.append(_kv([
        ("FEMA Flood Map Service",   "https://msc.fema.gov/portal/home"),
        ("FEMA NFHL Viewer",         "https://hazards-fema.maps.arcgis.com/apps/webappviewer/index.html"),
        ("EPA EnviroMapper",         "https://enviro.epa.gov/envirofacts/geo/mapper"),
        ("TX CEQ Remediation Sites", "https://www.tceq.texas.gov/remediation/locator"),
    ]))

    # ════════════════════════════════════════════════════════════════════════
    # PAGE 11 — DEMOGRAPHICS & NEIGHBORHOOD
    # ════════════════════════════════════════════════════════════════════════
    e.append(PageBreak())
    e.append(_h2("Demographics & Neighborhood Analysis"))
    e.append(_hr(BLUE, 1.5, 2, 8))

    if dem.get("population"):
        e.append(_tile4([
            (f"{int(dem.get('population',0)):,}",              "POPULATION",       f"ZIP {dem.get('zip','')}",  BLUE),
            (dem.get("median_household_income_fmt","N/A"),     "MEDIAN HH INCOME", "Census ACS",               TEAL),
            (f"{dem.get('owner_occupied_pct',0):.0f}%",        "OWNER OCCUPIED",   "% of housing units",       PURPLE),
            (str(dem.get("median_age","N/A")),                 "MEDIAN AGE",       "Residents",                ORANGE),
        ], {}))
        e.append(Spacer(1,10))

        e.append(_sec("Full Census Data (ACS 5-Year Estimate)"))
        e.append(_kv([
            ("ZIP Code",                  dem.get("zip")),
            ("Total Population",          f"{int(dem.get('population',0)):,}" if dem.get("population") else None),
            ("Median Household Income",   dem.get("median_household_income_fmt")),
            ("Median Household Income (Raw)", f"${dem.get('median_household_income',0):,.0f}" if dem.get("median_household_income") else None),
            ("Owner-Occupied Units",      f"{int(dem.get('owner_occupied_units',0)):,}" if dem.get("owner_occupied_units") else None),
            ("Total Occupied Units",      f"{int(dem.get('total_occupied_units',0)):,}" if dem.get("total_occupied_units") else None),
            ("Owner-Occupied %",          f"{dem.get('owner_occupied_pct',0):.1f}%"),
            ("Median Age",                str(dem.get("median_age")) if dem.get("median_age") else None),
            ("Unemployment Rate",         f"{dem.get('unemployment_rate',0):.1f}%" if dem.get("unemployment_rate") is not None else None),
            ("Source",                    dem.get("source","U.S. Census Bureau ACS")),
        ]))

        e.append(_sec("Key Metrics — Visual"))
        bars = []
        if dem.get("owner_occupied_pct") is not None:
            bars.append(_progress_bar("Owner-Occupied Housing",  float(dem["owner_occupied_pct"]), BLUE, W))
        if dem.get("unemployment_rate") is not None:
            bars.append(_progress_bar("Unemployment Rate",       float(dem["unemployment_rate"]),  RED_,  W))
        if bars:
            for bar in bars:
                e.append(bar)
        e.append(Spacer(1,5))
        e.append(_P(f"Source: {dem.get('source','U.S. Census Bureau ACS 5-Year Estimates')}  ·  ZIP {dem.get('zip','')}",
                    fontSize=7.5, textColor=GRAY))

    e.append(_sec("Walk Score"))
    if ws.get("walk_score") or ws.get("transit_score"):
        e.append(_kv([
            ("Walk Score",    str(ws.get("walk_score","N/A"))),
            ("Transit Score", str(ws.get("transit_score","N/A"))),
            ("Bike Score",    str(ws.get("bike_score","N/A"))),
            ("Description",   ws.get("walk_description")),
        ]))
    else:
        e.append(_P("Walk Score not available for this address. Set WALKSCORE_API_KEY for walkability data (free tier).",
                    fontSize=8.5, textColor=GRAY, leading=12))

    # ════════════════════════════════════════════════════════════════════════
    # PAGE 12 — RESEARCH, PERMITS & DUE DILIGENCE
    # ════════════════════════════════════════════════════════════════════════
    e.append(PageBreak())
    e.append(_h2("Research, Permits & Due Diligence"))
    e.append(_hr(BLUE, 1.5, 2, 8))

    enc_addr = urllib.parse.quote(raw_addr + " TX")
    zip_code = dem.get("zip","")

    e.append(_sec("Permit Portal"))
    e.append(_kv([
        ("City Permit Portal", permit_.get("permit","https://www.garlandtx.gov/building")),
        ("County Assessor",    permit_.get("assessor","https://www.dallascad.org")),
        ("City",               permit_.get("city")),
        ("State",              permit_.get("state","TX")),
    ]))
    e.append(Spacer(1,4))
    e.append(_pending_box("Permit History (ATTOM)", [
        "All permit types: Solar, Pool, Roof, HVAC, Electrical, Bathroom, Addition",
        "Permit dates, job values, contractor information",
        "Inspection status and improvement history",
        "Total capital improvement spend over property lifetime",
    ]))

    e.append(_sec("Comparable Sales Research"))
    e.append(_kv([
        ("Zillow Recently Sold", f"https://www.zillow.com/homes/recently_sold/{enc_addr}_rb/"),
        ("Redfin Sold Comps",    "https://www.redfin.com/TX/Garland"),
        ("LoopNet (Commercial)", f"https://www.loopnet.com/search/commercial-real-estate/garland-tx/for-sale/"),
        ("CoStar",               "https://www.costar.com"),
    ]))

    e.append(_sec("County & Government Records"))
    e.append(_kv([
        ("Dallas CAD Property Record", f"https://www.dallascad.org/AcctDetailRes.aspx"),
        ("Dallas County Appraisal Dist","https://www.dallascad.org"),
        ("Dallas County Deed Records", "https://www.dallascounty.org/departments/records/"),
        ("TX Secretary of State",      ent.get("manual_url","https://www.sos.state.tx.us")),
        ("Census Data (ZIP)",          f"https://data.census.gov/cedsci/table?g=860XX00US{zip_code}"),
        ("FEMA Flood Map",             "https://msc.fema.gov/portal/home"),
        ("EPA EnviroMapper",           "https://enviro.epa.gov/envirofacts/geo/mapper"),
    ]))

    e.append(_sec("Due Diligence Checklist"))
    checklist = [
        ("☐", "Order Phase I Environmental Study — required for commercial financing"),
        ("☐", "Pull full title search — confirm no liens, easements, or encumbrances"),
        ("☐", "Request current rent roll and lease abstracts from seller"),
        ("☐", "Obtain T-12 income statement and last 2 years P&L"),
        ("☐", "Verify actual NOI and cap rate against seller's stated numbers"),
        ("☐", "Walk property — note deferred maintenance, HVAC age, roof condition"),
        ("☐", "Confirm zoning compliance with current use"),
        ("☐", "Pull all open permits — confirm no unpermitted work"),
        ("☐", "Verify flood insurance requirement and cost with insurance agent"),
        ("☐", "Order survey if boundary lines are unclear"),
        ("☐", "Confirm property tax amounts directly with Dallas CAD"),
        ("☐", "Review utility bills for last 12 months"),
    ]
    cl_data = [[_PB(chk, fontSize=9, textColor=BLUE), _P(item, fontSize=8.5, textColor=DARK, leading=12)]
               for chk, item in checklist]
    cl_tbl = Table(cl_data, colWidths=[0.3*inch, W - 0.35*inch])
    cl_tbl.setStyle(TableStyle([
        ("ROWBACKGROUNDS", (0,0),(-1,-1), [WHITE, GHOST2]),
        ("GRID",           (0,0),(-1,-1), 0.4, BORDER),
        ("LEFTPADDING",    (0,0),(-1,-1), 6),
        ("TOPPADDING",     (0,0),(-1,-1), 5),
        ("BOTTOMPADDING",  (0,0),(-1,-1), 5),
        ("VALIGN",         (0,0),(-1,-1), "MIDDLE"),
    ]))
    e.append(cl_tbl)

    # ════════════════════════════════════════════════════════════════════════
    # PAGE 13 — DATA SOURCES & METHODOLOGY
    # ════════════════════════════════════════════════════════════════════════
    e.append(PageBreak())
    e.append(_h2("Data Sources & Methodology"))
    e.append(_hr(BLUE, 1.5, 2, 8))

    e.append(_sec("Active Data Sources"))
    sources = [
        ("Regrid Parcel API",          "Parcel data, owner info, assessment values, zoning — 7 DFW counties", "Active"),
        ("FEMA NFHL (ArcGIS REST)",    "National Flood Hazard Layer — flood zone by lat/lng",               "Active"),
        ("U.S. Census Bureau ACS",     "5-Year American Community Survey — demographics by ZIP code",       "Active"),
        ("Texas Secretary of State",   "Entity lookup by business name — status, registered agent",         "Active (limited)"),
        ("People Data Labs (PDL)",     "Skip trace — individual owner phone/email enrichment",              "Active (Pro)"),
        ("Dallas CAD (DCAD)",          "Supplemental Dallas County tax detail when available",              "Active (DFW)"),
        ("ATTOM Data Solutions",       "AVM, sold comps, mortgage, liens, permits, history",                "Pending API Key"),
        ("Walk Score API",             "Walk, transit, and bike scores",                                    "Pending API Key"),
    ]
    src_rows = [["Source", "Data Provided", "Status"]]
    for name, detail, status in sources:
        src_rows.append([name, detail, status])
    src_t = Table(src_rows, colWidths=[2.0*inch, 3.8*inch, 1.3*inch], repeatRows=1)
    src_t.setStyle(TableStyle([
        ("BACKGROUND",    (0,0),(-1,0),  DARK),
        ("TEXTCOLOR",     (0,0),(-1,0),  WHITE),
        ("FONTNAME",      (0,0),(-1,0),  "Helvetica-Bold"),
        ("FONTSIZE",      (0,0),(-1,0),  7.5),
        ("ROWBACKGROUNDS",(0,1),(-1,-1), [WHITE, GHOST2]),
        ("FONTNAME",      (0,1),(-1,-1), "Helvetica"),
        ("FONTSIZE",      (0,1),(-1,-1), 8),
        ("GRID",          (0,0),(-1,-1), 0.4, BORDER),
        ("LEFTPADDING",   (0,0),(-1,-1), 6),
        ("TOPPADDING",    (0,0),(-1,-1), 4),
        ("BOTTOMPADDING", (0,0),(-1,-1), 4),
        ("TEXTCOLOR",     (2,1),(2,-1),  GREEN),
        ("FONTNAME",      (2,1),(2,-1),  "Helvetica-Bold"),
    ]))
    e.append(src_t)

    e.append(_sec("Financial Estimate Methodology"))
    e.append(_kv([
        ("Property Tax Rate",       "Dallas County effective rates: ~2.25% residential / ~1.95% commercial"),
        ("Market Value Estimate",   "Assessed value × DFW market multiplier by use type (residential 1.05–1.25×, commercial 1.00–1.15×)"),
        ("AVM (when active)",       "ATTOM Automated Valuation Model — machine learning on comparable sales"),
        ("Expense Ratio",           "65% (DFW commercial average) — used to estimate NOI from GSI"),
        ("Market Rents",            "Industrial $6–$11/SF, Retail $14–$28/SF, Office $16–$30/SF (DFW 2025 avg)"),
        ("Implied Cap Rate",        "NOI midpoint ÷ market value midpoint × 100"),
        ("Loan Scenarios",          "7.0% rate, 25-year amortization, 25% down payment (user-adjustable)"),
        ("All estimates",           "For planning purposes only — not a certified appraisal"),
    ]))

    e.append(_sec("Motivation Score Methodology"))
    e.append(_kv([
        ("Scoring Range",           "0–100 (LOW: 0–30, MEDIUM: 31–60, HIGH: 61–100)"),
        ("Data Sources",            "Exclusively verified public records — no subjective inputs"),
        ("Absentee Owner",          "+25 pts — mailing address differs from property"),
        ("Out-of-State Owner",      "+20 pts — owner mailing is outside Texas"),
        ("LLC / Entity Ownership",  "+10 pts — corporate ownership, harder to reach"),
        ("Tax Delinquency",         "+20 pts — past due taxes signal financial stress"),
        ("Long Hold Duration",      "+15 pts — 10+ years, likely significant equity built"),
        ("Extended DOM",            "+10 pts — 90+ days on market suggests soft demand"),
        ("Price Reduction",         "+10 pts — documented price drop shows seller motivation"),
        ("Deed date",               "Required for hold duration — from ATTOM when available"),
    ]))

    # ════════════════════════════════════════════════════════════════════════
    # FINAL PAGE — DISCLAIMER
    # ════════════════════════════════════════════════════════════════════════
    e.append(PageBreak())
    e.append(_h2("Legal Disclaimer & Terms of Use"))
    e.append(_hr(BLUE, 1.5, 2, 8))
    e.append(Spacer(1, 8))

    disc_text = """
<b>PropIntel Report — Legal Disclaimer</b>

This report is generated from publicly available data sources including county appraisal districts,
FEMA National Flood Hazard Layer, U.S. Census Bureau American Community Survey (ACS),
Texas Secretary of State, ATTOM Data Solutions, and People Data Labs.

<b>Not an Appraisal.</b> This report does not constitute a certified real estate appraisal.
Market value estimates and AVM figures are computer-generated projections and may not
reflect actual market conditions. Do not use this report as a substitute for a licensed
MAI-certified appraisal.

<b>Not Investment, Legal, or Financial Advice.</b> PropIntel is not a licensed real estate broker,
appraiser, lender, attorney, or financial advisor. Nothing in this report constitutes
investment advice, legal advice, or financial advice. All information is provided for
informational purposes only.

<b>Financial Estimates.</b> Tax estimates, NOI projections, DSCR calculations, and cash-on-cash
return estimates are mathematical projections based on published market averages and stated
assumptions. Actual results will vary materially. Always verify with actual tax records,
certified rent rolls, audited financials, and a licensed accountant before making
investment decisions.

<b>Data Accuracy.</b> While PropIntel aggregates data from multiple authoritative sources,
we do not warrant the accuracy, completeness, or timeliness of any data. Public records
may contain errors. Assessment data reflects the most recent county filing which may
lag market conditions by 12–24 months.

<b>Skip Trace & Contact Data.</b> Owner contact information (where provided) is sourced from
public records and data aggregators. Users are solely responsible for compliance with
all applicable laws including the Telephone Consumer Protection Act (TCPA), CAN-SPAM Act,
Texas Business & Commerce Code, and all applicable real estate solicitation rules.

<b>Verify Before Closing.</b> All data in this report must be independently verified with
the relevant county, lender, title company, and licensed professionals before any
real estate transaction is executed.

propertyvalueintel.com  ·  All rights reserved.
"""
    e.append(Paragraph(disc_text, ParagraphStyle("disc", fontName="Helvetica", fontSize=8.5,
                                                  textColor=MID, leading=14, spaceAfter=6)))

    doc.build(e)
    return buf.getvalue()
