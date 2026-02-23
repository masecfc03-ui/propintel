"""
PropIntel PDF Report Builder — reportlab (zero system deps, works on Render free tier)

Generates a clean, professional PDF from report data.
Attached to delivery emails so customers have an offline copy.
"""

import io
import logging
from datetime import datetime

log = logging.getLogger(__name__)

# reportlab imports
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    HRFlowable, KeepTogether
)
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT

# ─── COLORS ────────────────────────────────────────────────────────────────
BLUE      = colors.HexColor("#2563eb")
DARK      = colors.HexColor("#0f172a")
GRAY      = colors.HexColor("#64748b")
LIGHT_BG  = colors.HexColor("#f8fafc")
BORDER    = colors.HexColor("#e2e8f0")
GREEN     = colors.HexColor("#16a34a")
AMBER     = colors.HexColor("#d97706")
RED_      = colors.HexColor("#dc2626")
WHITE     = colors.white


def _styles():
    base = getSampleStyleSheet()
    return {
        "title": ParagraphStyle("title", fontName="Helvetica-Bold", fontSize=20,
                                textColor=DARK, spaceAfter=4, leading=24),
        "subtitle": ParagraphStyle("subtitle", fontName="Helvetica", fontSize=11,
                                   textColor=GRAY, spaceAfter=16),
        "section": ParagraphStyle("section", fontName="Helvetica-Bold", fontSize=9,
                                  textColor=BLUE, spaceBefore=16, spaceAfter=6,
                                  textTransform="uppercase", letterSpacing=1),
        "body": ParagraphStyle("body", fontName="Helvetica", fontSize=9,
                               textColor=DARK, leading=14),
        "label": ParagraphStyle("label", fontName="Helvetica-Bold", fontSize=8,
                                textColor=GRAY, leading=12),
        "value": ParagraphStyle("value", fontName="Helvetica", fontSize=9,
                                textColor=DARK, leading=12),
        "footer": ParagraphStyle("footer", fontName="Helvetica", fontSize=7,
                                 textColor=GRAY, alignment=TA_CENTER),
        "badge": ParagraphStyle("badge", fontName="Helvetica-Bold", fontSize=8,
                                textColor=WHITE, alignment=TA_CENTER),
        "verdict": ParagraphStyle("verdict", fontName="Helvetica-Bold", fontSize=18,
                                  textColor=GREEN, alignment=TA_CENTER, leading=22),
    }


def _row(label, value, style):
    """Single data row for a table."""
    return [
        Paragraph(label, style["label"]),
        Paragraph(str(value) if value else "—", style["value"]),
    ]


def _table(rows, col_widths=(2.2*inch, 4.3*inch)):
    t = Table(rows, colWidths=col_widths)
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), LIGHT_BG),
        ("ROWBACKGROUNDS", (0, 0), (-1, -1), [WHITE, LIGHT_BG]),
        ("GRID", (0, 0), (-1, -1), 0.5, BORDER),
        ("LEFTPADDING", (0, 0), (-1, -1), 8),
        ("RIGHTPADDING", (0, 0), (-1, -1), 8),
        ("TOPPADDING", (0, 0), (-1, -1), 6),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
    ]))
    return t


def generate_pdf_bytes(report: dict) -> bytes:
    """
    Generate a PDF from a PropIntel report dict.
    Returns raw PDF bytes for email attachment or file save.
    """
    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf,
        pagesize=letter,
        rightMargin=0.75*inch,
        leftMargin=0.75*inch,
        topMargin=0.75*inch,
        bottomMargin=0.75*inch,
        title="PropIntel Report",
    )

    s = _styles()
    elements = []

    p = report.get("parcel", {}) or {}
    m = report.get("market_estimate", {}) or {}
    f = report.get("financials", {}) or {}
    flood = report.get("flood", {}) or {}
    demo = report.get("demographics", {}) or {}
    inv = report.get("investment_summary", {}) or {}
    entity = report.get("owner_entity", {}) or {}
    skip = report.get("skip_trace", {}) or {}
    tier = report.get("tier", "starter")
    address = p.get("address") or report.get("input", "Unknown address")
    generated = report.get("generated_at", datetime.utcnow().isoformat())[:10]

    # ── HEADER ──────────────────────────────────────────────────────────────
    elements.append(Paragraph("PropIntel", s["title"]))
    elements.append(Paragraph(
        f"{'Full Intel' if tier == 'pro' else 'Public Record'} Report  ·  Generated {generated}",
        s["subtitle"]
    ))
    elements.append(HRFlowable(width="100%", thickness=1, color=BLUE, spaceAfter=12))

    # Property address banner
    addr_table = Table(
        [[Paragraph(address, ParagraphStyle("addr", fontName="Helvetica-Bold",
                                            fontSize=13, textColor=DARK, leading=16))]],
        colWidths=[6.5*inch]
    )
    addr_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), LIGHT_BG),
        ("GRID", (0, 0), (-1, -1), 0.5, BORDER),
        ("LEFTPADDING", (0, 0), (-1, -1), 12),
        ("TOPPADDING", (0, 0), (-1, -1), 10),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 10),
        ("ROUNDEDCORNERS", [4]),
    ]))
    elements.append(addr_table)
    elements.append(Spacer(1, 16))

    # ── PARCEL & OWNERSHIP ─────────────────────────────────────────────────
    elements.append(Paragraph("Parcel & Ownership", s["section"]))
    parcel_rows = [
        _row("Owner", p.get("owner_name"), s),
        _row("Parcel (APN)", p.get("apn") or p.get("parcel_id"), s),
        _row("Property Address", p.get("address"), s),
        _row("County", p.get("county"), s),
        _row("Use Type", p.get("usetype") or p.get("use_type"), s),
        _row("Zoning", p.get("zoning"), s),
        _row("Building SF", f"{int(p['building_sf']):,}" if p.get("building_sf") else None, s),
        _row("Lot SF", f"{int(p['lot_sf']):,}" if p.get("lot_sf") else None, s),
        _row("Year Built", p.get("year_built"), s),
        _row("Owner Mailing", f"{p.get('owner_mailing_address', '')} {p.get('owner_mailing_city', '')} {p.get('owner_mailing_state', '')}".strip() or None, s),
        _row("Absentee Owner", "YES — mailing address differs from property" if p.get("absentee") else "No", s),
        _row("Out-of-State", "YES" if p.get("out_of_state") else "No", s),
    ]
    elements.append(_table([r for r in parcel_rows if r[1].text != "—"]))

    # ── VALUATION ──────────────────────────────────────────────────────────
    elements.append(Spacer(1, 4))
    elements.append(Paragraph("Valuation", s["section"]))
    val_rows = [
        _row("County Assessed (Tax Value)", f"${p.get('assessed_total', 0):,.0f}" if p.get("assessed_total") else None, s),
        _row("Land Value", f"${p.get('land_value', 0):,.0f}" if p.get("land_value") else None, s),
        _row("Improvement Value", f"${p.get('improvement_value', 0):,.0f}" if p.get("improvement_value") else None, s),
        _row("Est. Market Range", m.get("range_fmt"), s),
        _row("Market Estimate Method", f"DFW {m.get('property_type', '')} multiplier ({m.get('multiplier_low', '')}–{m.get('multiplier_high', '')}x assessed)" if m.get("multiplier_low") else None, s),
    ]
    elements.append(_table([r for r in val_rows if r[1].text != "—"]))

    # ── FINANCIAL ESTIMATES ────────────────────────────────────────────────
    if f.get("available"):
        elements.append(Spacer(1, 4))
        elements.append(Paragraph("Financial Estimates", s["section"]))
        fin_rows = [
            _row("Est. Annual Property Tax", f"${f.get('est_annual_tax', 0):,.0f}", s),
            _row("Est. Monthly Tax", f"${f.get('est_monthly_tax', 0):,.0f}", s),
            _row("Effective Tax Rate", f"{f.get('tax_rate_pct', 0)}%", s),
        ]
        if f.get("cash_flow"):
            fin_rows += [
                _row("Market Rent Range", f.get("rent_per_sf_range"), s),
                _row("Use Type", f.get("rent_use_label"), s),
                _row("Gross Income (GSI)", f.get("gsi_range"), s),
                _row("Net Operating Income", f.get("noi_range"), s),
                _row("Implied Cap Rate", f"{f.get('implied_cap_rate', 0)}%", s),
            ]
        elements.append(_table([r for r in fin_rows if r[1].text != "—"]))

    # ── FLOOD & ENVIRONMENT ────────────────────────────────────────────────
    elements.append(Spacer(1, 4))
    elements.append(Paragraph("FEMA Flood Zone", s["section"]))
    flood_rows = [
        _row("Flood Zone", flood.get("zone"), s),
        _row("Zone Description", flood.get("zone_description") or flood.get("summary"), s),
        _row("Risk Level", flood.get("risk_level"), s),
        _row("Source", "FEMA National Flood Hazard Layer (NFHL)", s),
    ]
    elements.append(_table([r for r in flood_rows if r[1].text != "—"]))

    # ── OWNER ENTITY ───────────────────────────────────────────────────────
    if entity and not entity.get("is_individual") and not entity.get("error"):
        elements.append(Spacer(1, 4))
        elements.append(Paragraph("Owner Entity Intelligence (TX SOS)", s["section"]))
        ent_rows = [
            _row("Entity Name", entity.get("entity_name"), s),
            _row("TX SOS Status", entity.get("status"), s),
            _row("Formation Date", entity.get("formation_date"), s),
            _row("Registered Agent", entity.get("registered_agent"), s),
        ]
        elements.append(_table([r for r in ent_rows if r[1].text != "—"]))

    # ── PRO: SKIP TRACE ────────────────────────────────────────────────────
    if tier == "pro" and skip.get("status") == "hit":
        elements.append(Spacer(1, 4))
        elements.append(Paragraph("Owner Contact (Skip Trace)", s["section"]))
        phones = skip.get("phones", [])
        emails = skip.get("emails", [])
        ct_rows = [
            _row("Phone(s)", ", ".join(phones[:3]) if phones else None, s),
            _row("Email(s)", ", ".join(emails[:3]) if emails else None, s),
        ]
        elements.append(_table([r for r in ct_rows if r[1].text != "—"]))

    # ── PRO: INVESTMENT VERDICT ────────────────────────────────────────────
    if tier == "pro" and inv.get("verdict"):
        elements.append(Spacer(1, 8))
        verdict_color = GREEN if "STRONG" in (inv.get("verdict") or "").upper() else (AMBER if "INVESTIGATE" in (inv.get("verdict") or "").upper() else RED_)
        verdict_table = Table(
            [[Paragraph(inv.get("verdict", ""), ParagraphStyle(
                "vv", fontName="Helvetica-Bold", fontSize=16,
                textColor=verdict_color, alignment=TA_CENTER, leading=20
            ))]],
            colWidths=[6.5*inch]
        )
        verdict_table.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, -1), LIGHT_BG),
            ("GRID", (0, 0), (-1, -1), 1, verdict_color),
            ("TOPPADDING", (0, 0), (-1, -1), 14),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 14),
        ]))
        elements.append(Paragraph("Investment Verdict", s["section"]))
        elements.append(verdict_table)
        if inv.get("summary"):
            elements.append(Spacer(1, 6))
            elements.append(Paragraph(inv["summary"], s["body"]))

    # ── DEMOGRAPHICS ───────────────────────────────────────────────────────
    if demo.get("population"):
        elements.append(Spacer(1, 4))
        elements.append(Paragraph("Neighborhood Demographics (Census ACS)", s["section"]))
        dem_rows = [
            _row("Population", f"{int(demo.get('population', 0)):,}" if demo.get("population") else None, s),
            _row("Median Household Income", demo.get("median_household_income_fmt"), s),
            _row("Owner-Occupied Housing", f"{demo.get('owner_occupied_pct', 0)}%" if demo.get("owner_occupied_pct") else None, s),
            _row("Median Age", str(demo.get("median_age")) if demo.get("median_age") else None, s),
        ]
        elements.append(_table([r for r in dem_rows if r[1].text != "—"]))

    # ── DISCLAIMER ─────────────────────────────────────────────────────────
    elements.append(Spacer(1, 20))
    elements.append(HRFlowable(width="100%", thickness=0.5, color=BORDER))
    elements.append(Spacer(1, 8))
    elements.append(Paragraph(
        "This report contains publicly available data from government sources including county appraisal districts, "
        "FEMA, U.S. Census Bureau, and Texas Secretary of State. Financial estimates are projections based on "
        "published market data and are not appraisals. PropIntel is not a licensed broker, appraiser, or attorney. "
        "Not investment advice. All values should be independently verified before making investment decisions.",
        s["footer"]
    ))

    doc.build(elements)
    return buf.getvalue()
