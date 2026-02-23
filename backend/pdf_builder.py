"""
PropIntel PDF Report Builder — reportlab (zero system deps, Render-compatible)

Uses ACTUAL pipeline key names (verified against live pipeline output).
Sections: Header, Parcel, Valuation, Financials, Deal Analysis,
          Motivation Score, Flags/Signals, Skip Trace, FEMA Flood,
          Demographics, Disclaimer
"""

import io
import logging
from datetime import datetime

from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, HRFlowable
)
from reportlab.lib.enums import TA_LEFT, TA_CENTER

log = logging.getLogger(__name__)

# ── PALETTE ──────────────────────────────────────────────────────────────────
BLUE     = colors.HexColor("#2563eb")
DARK     = colors.HexColor("#0f172a")
GRAY     = colors.HexColor("#64748b")
LIGHT_BG = colors.HexColor("#f8fafc")
BORDER   = colors.HexColor("#e2e8f0")
GREEN    = colors.HexColor("#16a34a")
AMBER    = colors.HexColor("#d97706")
RED_C    = colors.HexColor("#dc2626")
GREEN_BG = colors.HexColor("#f0fdf4")
AMBER_BG = colors.HexColor("#fffbeb")
RED_BG   = colors.HexColor("#fef2f2")
WHITE    = colors.white

W = 6.5 * inch   # usable page width

# ── STYLES ───────────────────────────────────────────────────────────────────
def _s():
    return {
        "title":   ParagraphStyle("title",   fontName="Helvetica-Bold", fontSize=22,
                                  textColor=DARK, spaceAfter=4, leading=26),
        "tagline": ParagraphStyle("tagline", fontName="Helvetica", fontSize=10,
                                  textColor=GRAY, spaceAfter=14),
        "addr":    ParagraphStyle("addr",    fontName="Helvetica-Bold", fontSize=13,
                                  textColor=DARK, leading=16),
        "section": ParagraphStyle("section", fontName="Helvetica-Bold", fontSize=8,
                                  textColor=BLUE, spaceBefore=14, spaceAfter=5,
                                  leading=10),
        "label":   ParagraphStyle("label",   fontName="Helvetica-Bold", fontSize=8,
                                  textColor=GRAY, leading=11),
        "value":   ParagraphStyle("value",   fontName="Helvetica", fontSize=9,
                                  textColor=DARK, leading=12),
        "note":    ParagraphStyle("note",    fontName="Helvetica", fontSize=7,
                                  textColor=GRAY, leading=10),
        "flag_g":  ParagraphStyle("flag_g",  fontName="Helvetica", fontSize=8,
                                  textColor=GREEN, leading=11),
        "flag_y":  ParagraphStyle("flag_y",  fontName="Helvetica", fontSize=8,
                                  textColor=AMBER, leading=11),
        "flag_r":  ParagraphStyle("flag_r",  fontName="Helvetica", fontSize=8,
                                  textColor=RED_C, leading=11),
        "score":   ParagraphStyle("score",   fontName="Helvetica-Bold", fontSize=26,
                                  textColor=DARK, alignment=TA_CENTER, leading=30),
        "verdict": ParagraphStyle("verdict", fontName="Helvetica-Bold", fontSize=14,
                                  textColor=GREEN, alignment=TA_CENTER, leading=18),
        "footer":  ParagraphStyle("footer",  fontName="Helvetica", fontSize=7,
                                  textColor=GRAY, alignment=TA_CENTER, leading=10),
    }


def _r(label, value, s, col_w=(2.0*inch, 4.5*inch)):
    """Single key-value row."""
    v = str(value) if value is not None else "—"
    if v == "" or v == "None":
        v = "—"
    return [Paragraph(label, s["label"]), Paragraph(v, s["value"])]


def _tbl(rows, col_w=(2.0*inch, 4.5*inch)):
    """Styled 2-column data table, skips rows with '—' values."""
    filtered = [r for r in rows if r[1].text != "—"]
    if not filtered:
        return None
    t = Table(filtered, colWidths=col_w)
    t.setStyle(TableStyle([
        ("ROWBACKGROUNDS", (0, 0), (-1, -1), [WHITE, LIGHT_BG]),
        ("GRID", (0, 0), (-1, -1), 0.4, BORDER),
        ("LEFTPADDING",  (0, 0), (-1, -1), 7),
        ("RIGHTPADDING", (0, 0), (-1, -1), 7),
        ("TOPPADDING",   (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING",(0, 0), (-1, -1), 5),
        ("VALIGN",       (0, 0), (-1, -1), "TOP"),
    ]))
    return t


def _section(label, s):
    return Paragraph(label.upper(), s["section"])


def _fmt_bool(v):
    if v is True:  return "Yes"
    if v is False: return "No"
    return "—"


def generate_pdf_bytes(report: dict) -> bytes:
    """
    Generate a full PropIntel PDF from a pipeline report dict.
    Returns raw PDF bytes.
    """
    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=letter,
        rightMargin=0.75*inch, leftMargin=0.75*inch,
        topMargin=0.75*inch,   bottomMargin=0.75*inch,
        title="PropIntel Report",
    )

    s = _s()
    e = []   # elements list

    # shortcuts — use actual pipeline key names
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
    tier = report.get("tier", "starter")
    addr = p.get("property_address") or report.get("input", "Unknown")
    gen  = (report.get("generated_at") or datetime.utcnow().isoformat())[:10]

    # ── HEADER ───────────────────────────────────────────────────────────────
    e.append(Paragraph("PropIntel", s["title"]))
    e.append(Paragraph(
        f"{'Full Intel' if tier == 'pro' else 'Public Record'} Report  ·  {gen}",
        s["tagline"]
    ))
    e.append(HRFlowable(width=W, thickness=1.5, color=BLUE, spaceAfter=10))

    # Address banner
    ab = Table([[Paragraph(addr + ("  |  " + p.get("county","").title() + " County" if p.get("county") else ""), s["addr"])]],
               colWidths=[W])
    ab.setStyle(TableStyle([
        ("BACKGROUND",    (0,0),(-1,-1), LIGHT_BG),
        ("GRID",          (0,0),(-1,-1), 0.5, BORDER),
        ("LEFTPADDING",   (0,0),(-1,-1), 10),
        ("TOPPADDING",    (0,0),(-1,-1), 10),
        ("BOTTOMPADDING", (0,0),(-1,-1), 10),
    ]))
    e.append(ab)
    e.append(Spacer(1, 10))

    # ── SIGNALS / FLAGS ──────────────────────────────────────────────────────
    if flgs:
        e.append(_section("Investment Signals", s))
        flag_rows = []
        for f in flgs:
            t = f.get("type", "green")
            txt = f.get("text", "")
            icon = "✓" if t == "green" else ("⚠" if t == "yellow" else "✗")
            st = s["flag_g"] if t == "green" else (s["flag_y"] if t == "yellow" else s["flag_r"])
            flag_rows.append([Paragraph(f"{icon}  {txt}", st)])
        ft = Table(flag_rows, colWidths=[W])
        ft.setStyle(TableStyle([
            ("ROWBACKGROUNDS", (0,0),(-1,-1), [WHITE, LIGHT_BG]),
            ("GRID",           (0,0),(-1,-1), 0.4, BORDER),
            ("LEFTPADDING",    (0,0),(-1,-1), 8),
            ("TOPPADDING",     (0,0),(-1,-1), 5),
            ("BOTTOMPADDING",  (0,0),(-1,-1), 5),
        ]))
        e.append(ft)

    # ── PARCEL & OWNERSHIP ───────────────────────────────────────────────────
    e.append(_section("Parcel & Ownership", s))
    parcel_rows = [
        _r("Owner of Record",     p.get("owner_name"),           s),
        _r("Secondary Owner",     p.get("owner_name2"),          s),
        _r("APN / Parcel ID",     p.get("apn"),                  s),
        _r("Property Address",    p.get("property_address"),     s),
        _r("County / State",      f"{(p.get('county') or '').title()}, {p.get('state','')}", s),
        _r("Use Description",     p.get("use_description"),      s),
        _r("Zoning",              p.get("zoning"),               s),
        _r("Building SF",         f"{int(p['building_sf']):,}" if p.get("building_sf") else None, s),
        _r("Lot (Acres)",         p.get("lot_acres"),            s),
        _r("Year Built",          p.get("year_built"),           s),
        _r("Owner Mailing",       p.get("owner_mailing"),        s),
        _r("Owner City / State",  f"{p.get('owner_city','')}, {p.get('owner_state','')}" if p.get("owner_city") else None, s),
        _r("Absentee Owner",      "YES" if p.get("absentee_owner") else "No", s),
        _r("Out-of-State Owner",  "YES" if p.get("out_of_state_owner") else "No", s),
        _r("Tax Delinquent",      "YES" if p.get("tax_delinquent") else "No", s),
        _r("Data Source",         p.get("data_sources") or p.get("source"), s),
    ]
    t = _tbl(parcel_rows)
    if t: e.append(t)

    # ── VALUATION ────────────────────────────────────────────────────────────
    e.append(_section("Valuation", s))
    val_rows = [
        _r("County Assessed Total (Tax Value)", f"${p.get('assessed_total',0):,.0f}" if p.get("assessed_total") else None, s),
        _r("Assessed Land",        f"${p.get('assessed_land',0):,.0f}" if p.get("assessed_land") else None, s),
        _r("Assessed Improvement", f"${p.get('assessed_improvement',0):,.0f}" if p.get("assessed_improvement") else None, s),
        _r("YoY Change",           f"{p.get('assessed_yoy_pct')}%" if p.get("assessed_yoy_pct") is not None else None, s),
        _r("Est. Market Range",    mkt.get("range_fmt"), s),
        _r("Market Methodology",   mkt.get("methodology") or mkt.get("note"), s),
        _r("Confidence",           mkt.get("confidence"), s),
    ]
    t = _tbl(val_rows)
    if t: e.append(t)

    # ── FINANCIAL ESTIMATES ──────────────────────────────────────────────────
    if fin.get("available"):
        e.append(_section("Financial Estimates", s))
        fin_rows = [
            _r("Est. Annual Property Tax", f"${fin.get('est_annual_tax',0):,.0f}" if fin.get("est_annual_tax") else None, s),
            _r("Est. Monthly Tax",         f"${fin.get('est_monthly_tax',0):,.0f}" if fin.get("est_monthly_tax") else None, s),
            _r("Effective Tax Rate",       f"{fin.get('tax_rate_pct')}%" if fin.get("tax_rate_pct") else None, s),
        ]
        if fin.get("cash_flow"):
            fin_rows += [
                _r("Building SF",          f"{int(fin['building_sf']):,}" if fin.get("building_sf") else None, s),
                _r("Use Type",             fin.get("rent_use_label"), s),
                _r("Market Rent ($/SF/yr)",fin.get("rent_per_sf_range"), s),
                _r("Gross Income (GSI)",   fin.get("gsi_range"), s),
                _r("Net Oper. Income",     fin.get("noi_range"), s),
                _r("Implied Cap Rate",     f"{fin.get('implied_cap_rate')}%" if fin.get("implied_cap_rate") else None, s),
            ]
        t = _tbl(fin_rows)
        if t: e.append(t)
        # Tax note
        if fin.get("tax_note"):
            e.append(Spacer(1,4))
            e.append(Paragraph(f"⚠ {fin['tax_note']}", s["note"]))
        if fin.get("cash_flow_note"):
            e.append(Paragraph(f"⚠ {fin['cash_flow_note']}", s["note"]))

    # ── DEAL ANALYSIS ────────────────────────────────────────────────────────
    if da.get("asking_price") or da.get("dscr") or da.get("cash_on_cash_pct"):
        e.append(_section("Deal Analysis", s))
        loan = da.get("loan_assumptions") or {}
        da_rows = [
            _r("Asking Price",             da.get("asking_price_fmt"), s),
            _r("Price / SF",               f"${da.get('price_per_sf'):,.0f}/SF" if da.get("price_per_sf") else None, s),
            _r("Building SF",              f"{int(da['building_sf']):,}" if da.get("building_sf") else None, s),
            _r("Assessed vs Asking",       f"+{da.get('assessed_vs_asking_premium_pct')}% premium" if da.get("assessed_vs_asking_premium_pct") else None, s),
            _r("Stated Cap Rate",          f"{da.get('stated_cap_rate')}%" if da.get("stated_cap_rate") else None, s),
            _r("Stated NOI",               da.get("stated_noi_fmt"), s),
            _r("DSCR",                     f"{da.get('dscr'):.2f}x" if da.get("dscr") else None, s),
            _r("Cash-on-Cash Return",      f"{da.get('cash_on_cash_pct')}%" if da.get("cash_on_cash_pct") else None, s),
            _r("Monthly Debt Service",     f"${da.get('monthly_debt_service'):,.0f}/mo" if da.get("monthly_debt_service") else None, s),
            _r("Assumed LTV",              f"{loan.get('ltv_pct')}%" if loan.get("ltv_pct") else None, s),
            _r("Assumed Rate",             f"{loan.get('rate_pct')}%" if loan.get("rate_pct") else None, s),
            _r("Amortization",             f"{loan.get('amortization_years')} yrs" if loan.get("amortization_years") else None, s),
        ]
        t = _tbl(da_rows)
        if t: e.append(t)
        if da.get("note"):
            e.append(Spacer(1,4))
            e.append(Paragraph(f"⚠ {da['note']}", s["note"]))

    # ── MOTIVATION SCORE ─────────────────────────────────────────────────────
    if mot.get("score") is not None:
        e.append(_section("Seller Motivation Score", s))
        score_val  = mot.get("score", 0)
        score_tier = mot.get("tier", "LOW")
        score_color = GREEN if score_tier == "HIGH" else (AMBER if score_tier == "MEDIUM" else RED_C)
        bg_color    = GREEN_BG if score_tier == "HIGH" else (AMBER_BG if score_tier == "MEDIUM" else RED_BG)

        score_tbl = Table([
            [Paragraph(str(score_val), ParagraphStyle("sc", fontName="Helvetica-Bold",
                       fontSize=28, textColor=score_color, alignment=TA_CENTER, leading=32)),
             Paragraph(f"<b>{score_tier}</b><br/>{mot.get('interpretation','')[:120]}",
                       ParagraphStyle("si", fontName="Helvetica", fontSize=8,
                                      textColor=DARK, leading=12))]
        ], colWidths=[0.9*inch, 5.6*inch])
        score_tbl.setStyle(TableStyle([
            ("BACKGROUND",    (0,0),(-1,-1), bg_color),
            ("GRID",          (0,0),(-1,-1), 0.5, BORDER),
            ("LEFTPADDING",   (0,0),(-1,-1), 10),
            ("TOPPADDING",    (0,0),(-1,-1), 10),
            ("BOTTOMPADDING", (0,0),(-1,-1), 10),
            ("VALIGN",        (0,0),(-1,-1), "MIDDLE"),
        ]))
        e.append(score_tbl)

        # Triggered indicators
        triggered = [i for i in mot.get("indicators", []) if i.get("triggered")]
        if triggered:
            e.append(Spacer(1,4))
            e.append(Paragraph("Triggered signals:", s["note"]))
            for ind in triggered:
                e.append(Paragraph(f"  • {ind['name']} (+{ind['points']}pts) — {ind.get('evidence','')}", s["note"]))
        not_triggered = [i for i in mot.get("indicators", []) if not i.get("triggered")]
        if not_triggered:
            e.append(Spacer(1,2))
            e.append(Paragraph("Not triggered: " + ", ".join(i["name"] for i in not_triggered), s["note"]))

    # ── OWNER ENTITY (TX SOS) ────────────────────────────────────────────────
    if ent.get("entity_name") and not ent.get("error"):
        e.append(_section("Owner Entity (TX Secretary of State)", s))
        ent_rows = [
            _r("Entity Name",       ent.get("entity_name"), s),
            _r("TX SOS Status",     ent.get("status"), s),
            _r("Formation Date",    ent.get("formation_date"), s),
            _r("Registered Agent",  ent.get("registered_agent"), s),
        ]
        t = _tbl(ent_rows)
        if t: e.append(t)
    elif ent.get("entity_name"):
        e.append(_section("Owner Entity", s))
        e.append(Paragraph(
            f"Entity: {ent['entity_name']}  |  TX SOS lookup blocked (403 — bot protection). "
            f"Search manually: {ent.get('manual_url','')}",
            s["note"]
        ))

    # ── SKIP TRACE ───────────────────────────────────────────────────────────
    if tier == "pro":
        e.append(_section("Skip Trace / Owner Contact", s))
        if sk.get("status") == "hit":
            phones = sk.get("phones", [])
            emails = sk.get("emails", [])
            st_rows = [
                _r("Phone(s)", ", ".join(phones[:4]) if phones else "None found", s),
                _r("Email(s)", ", ".join(emails[:4]) if emails else "None found", s),
                _r("Source",   sk.get("source"), s),
            ]
            t = _tbl(st_rows)
            if t: e.append(t)
        else:
            e.append(Paragraph(
                f"Status: {sk.get('status','—')}  |  {sk.get('note','')}",
                s["note"]
            ))

    # ── FEMA FLOOD ───────────────────────────────────────────────────────────
    e.append(_section("FEMA Flood Zone", s))
    flood_rows = [
        _r("Zone",        fld.get("zone"), s),
        _r("Description", fld.get("description"), s),
        _r("FIRM Panel",  fld.get("firm_panel"), s),
        _r("Insurance Required", _fmt_bool(fld.get("flood_insurance_required")), s),
        _r("Source",      fld.get("source"), s),
    ]
    t = _tbl(flood_rows)
    if t:
        e.append(t)
    elif fld.get("error"):
        e.append(Paragraph(f"Lookup error: {fld['error'][:120]}. Verify at msc.fema.gov", s["note"]))

    # ── DEMOGRAPHICS ─────────────────────────────────────────────────────────
    if dem.get("population"):
        e.append(_section("Neighborhood Demographics (Census ACS)", s))
        dem_rows = [
            _r("ZIP Code",              dem.get("zip"), s),
            _r("Population",            f"{int(dem['population']):,}", s),
            _r("Median HH Income",      dem.get("median_household_income_fmt"), s),
            _r("Owner-Occupied",        f"{dem.get('owner_occupied_pct')}%" if dem.get("owner_occupied_pct") else None, s),
            _r("Median Age",            str(dem.get("median_age")) if dem.get("median_age") else None, s),
            _r("Unemployment Rate",     f"{dem.get('unemployment_rate')}%" if dem.get("unemployment_rate") else None, s),
        ]
        t = _tbl(dem_rows)
        if t: e.append(t)

    # ── DISCLAIMER ───────────────────────────────────────────────────────────
    e.append(Spacer(1, 18))
    e.append(HRFlowable(width=W, thickness=0.5, color=BORDER))
    e.append(Spacer(1, 7))
    e.append(Paragraph(
        "This report contains publicly available data from government sources including county appraisal districts, "
        "FEMA, U.S. Census Bureau, and Texas Secretary of State. Financial estimates and deal analysis figures "
        "are projections based on published market data — not appraisals or audit-verified financials. "
        "PropIntel is not a licensed broker, appraiser, or attorney. Not investment advice. "
        "Verify all figures independently before making investment decisions.  "
        f"Generated {gen} · propertyvalueintel.com",
        s["footer"]
    ))

    doc.build(e)
    return buf.getvalue()
