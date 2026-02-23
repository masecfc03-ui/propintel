"""
Report Generator — takes pipeline output dict → renders full HTML report.
"""
from datetime import datetime


def _fmt(val, prefix="$", suffix=""):
    try:
        return f"{prefix}{int(val):,}{suffix}"
    except Exception:
        return str(val) if val else "N/A"

def _row(label, value, cls=""):
    c = f' class="{cls}"' if cls else ""
    return f'<div class="dr"><span class="dl">{label}</span><span class="dv{" " + cls if cls else ""}">{value}</span></div>'

def _color(flag):
    return {"green": "#22c55e", "yellow": "#f59e0b", "red": "#ef4444"}.get(flag, "#94a3b8")

def _bg(flag):
    return {"green": "rgba(34,197,94,0.08)", "yellow": "rgba(245,158,11,0.08)", "red": "rgba(239,68,68,0.08)"}.get(flag, "transparent")

def _border(flag):
    return {"green": "rgba(34,197,94,0.2)", "yellow": "rgba(245,158,11,0.2)", "red": "rgba(239,68,68,0.2)"}.get(flag, "transparent")


CSS = """
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
:root{--bg:#1a1a1a;--bg2:#242424;--bg3:#2e2e2e;--border:rgba(255,255,255,0.08);
      --text:#f1f5f9;--text2:#94a3b8;--text3:#64748b;--accent:#3b82f6;--green:#22c55e}
body{font-family:'Inter',system-ui,sans-serif;background:var(--bg);color:var(--text);line-height:1.6}
.topbar{background:var(--bg2);border-bottom:1px solid var(--border);padding:14px 32px;
        display:flex;align-items:center;justify-content:space-between}
.logo{font-size:1rem;font-weight:800;text-decoration:none;color:var(--text);display:flex;align-items:center;gap:8px}
.logo-dot{background:var(--accent);border-radius:6px;width:24px;height:24px;display:flex;align-items:center;justify-content:center;font-size:12px}
.tbar-right{display:flex;align-items:center;gap:16px}
.tbar-right a{color:var(--text2);text-decoration:none;font-size:0.82rem;font-weight:500}
.print-btn{background:none;border:1px solid var(--border);color:var(--text2);padding:7px 16px;
           border-radius:100px;font-size:0.78rem;font-weight:600;cursor:pointer}
.report{max-width:880px;margin:0 auto;padding:36px 24px 80px}
.rhead{background:var(--bg2);border:1px solid var(--border);border-radius:16px;padding:28px;margin-bottom:20px}
.tier-badge{display:inline-block;padding:4px 14px;border-radius:100px;font-size:0.72rem;font-weight:700;
            letter-spacing:1px;text-transform:uppercase;margin-bottom:16px;
            background:rgba(59,130,246,0.1);border:1px solid rgba(59,130,246,0.25);color:#3b82f6}
.prop-name{font-size:1.4rem;font-weight:800;letter-spacing:-0.5px;margin-bottom:4px}
.prop-addr{color:var(--text2);font-size:0.9rem;margin-bottom:16px}
.tags{display:flex;gap:8px;flex-wrap:wrap}
.tag{background:var(--bg3);border:1px solid var(--border);padding:4px 12px;border-radius:6px;font-size:0.75rem;color:var(--text2)}
.section{background:var(--bg2);border:1px solid var(--border);border-radius:14px;margin-bottom:16px;overflow:hidden}
.sec-head{padding:14px 22px;border-bottom:1px solid var(--border);display:flex;align-items:center;justify-content:space-between}
.sec-title{font-size:0.82rem;font-weight:700;text-transform:uppercase;letter-spacing:0.8px}
.sec-src{font-size:0.7rem;color:var(--text3)}
.sec-src a{color:#60a5fa;text-decoration:none}
.sec-body{padding:20px 22px}
.dr{display:flex;align-items:flex-start;justify-content:space-between;padding:9px 0;
    border-bottom:1px solid var(--border);gap:12px;flex-wrap:wrap}
.dr:last-child{border-bottom:none;padding-bottom:0}
.dl{font-size:0.8rem;color:var(--text3);font-weight:500;min-width:160px}
.dv{font-size:0.88rem;font-weight:600;text-align:right}
.dv.green{color:var(--green)}.dv.red{color:#ef4444}.dv.yellow{color:#f59e0b}
.dt{width:100%;border-collapse:collapse;font-size:0.82rem}
.dt th{text-align:left;color:var(--text3);font-size:0.7rem;font-weight:700;text-transform:uppercase;
       letter-spacing:0.5px;padding:8px 10px;border-bottom:1px solid var(--border);background:var(--bg3)}
.dt td{padding:10px 10px;border-bottom:1px solid var(--border);color:var(--text2)}
.dt tr:last-child td{border-bottom:none}
.flags{display:grid;grid-template-columns:1fr 1fr;gap:10px}
.pro-tag{background:rgba(34,197,94,0.12);color:var(--green);border:1px solid rgba(34,197,94,0.25);
         padding:2px 8px;border-radius:4px;font-size:0.65rem;font-weight:700;letter-spacing:0.5px;margin-left:8px}
.disc{background:var(--bg3);border:1px solid var(--border);border-radius:10px;padding:16px 20px;margin-top:16px}
.disc-t{font-size:0.7rem;font-weight:700;text-transform:uppercase;letter-spacing:0.8px;color:var(--text3);margin-bottom:6px}
.disc p{font-size:0.78rem;color:var(--text3);line-height:1.6}
@media print{body{background:white;color:#0f172a}.topbar{display:none}
  .section,.rhead{background:white;border-color:#e2e8f0}.sec-head{background:#f8fafc}}
"""


def generate_html(data: dict) -> str:
    tier = data.get("tier", "starter")
    is_pro = (tier == "pro")
    ts = data.get("generated_at", datetime.utcnow().isoformat())[:19].replace("T", " ") + " UTC"

    parcel   = data.get("parcel", {})
    listing  = data.get("listing", {})
    flood    = data.get("flood", {})
    demo     = data.get("demographics", {})
    biz_list = data.get("businesses", [])
    motivation = data.get("motivation", {})
    flags    = data.get("flags", [])
    geo      = data.get("geo", {})

    address   = str(data.get("resolved_address") or data.get("input") or "Unknown Address")
    owner     = str(parcel.get("owner_name") or "N/A")
    apn       = str(parcel.get("apn") or "N/A")
    zip_code  = str(geo.get("zip") or demo.get("zip") or "")
    county    = str(geo.get("county") or "")

    # Listing
    ask_price   = str(listing.get("asking_price_fmt") or _fmt(listing.get("asking_price")) or "N/A")
    cap_rate    = str(listing.get("cap_rate") or "N/A")
    bldg_sf     = str(listing.get("building_sf") or parcel.get("building_sf") or "N/A")
    broker      = str(listing.get("broker") or "N/A")
    dom         = listing.get("days_on_market", "N/A")
    listing_url = str(listing.get("url") or "#")
    source_site = str((listing.get("source_site") or "Listing")).title()

    # Parcel
    assessed_total = _fmt(parcel.get("assessed_total"))
    assessed_land  = _fmt(parcel.get("assessed_land"))
    assessed_imp   = _fmt(parcel.get("assessed_improvement"))
    delinquent     = parcel.get("tax_delinquent", False)
    year_built     = str(parcel.get("year_built") or "N/A")
    legal_desc     = str(parcel.get("legal_description") or "N/A")
    owner_mail     = str(parcel.get("owner_mailing") or "N/A")

    # Flood
    flood_zone    = str(flood.get("zone") or "N/A")
    flood_desc    = str(flood.get("description") or "")
    flood_panel   = str(flood.get("firm_panel") or "N/A")
    flood_date    = str(flood.get("effective_date") or "N/A")
    flood_ins_req = flood.get("flood_insurance_required", None)
    flood_src_url = str(flood.get("source_url") or "https://msc.fema.gov/portal/home")

    # Demo
    pop      = f"{demo.get('population', 0):,}" if demo.get("population") else "N/A"
    mhi      = str(demo.get("median_household_income_fmt") or "N/A")
    own_pct  = (str(demo.get("owner_occupied_pct")) + "%") if demo.get("owner_occupied_pct") else "N/A"
    med_age  = str(demo.get("median_age") or "N/A")
    demo_src = str(demo.get("source_url") or "https://data.census.gov")

    # Tier label
    tier_label = "Full Intel Report · $29.99" if is_pro else "Public Record Report · $9.99"
    pro_badge  = ' <span style="background:#22c55e;color:#022c22;padding:2px 10px;border-radius:100px;font-size:0.7rem;font-weight:800;margin-left:8px;">PRO</span>' if is_pro else ""

    # ── Build section strings ──────────────────────────────────────────────────

    # Listing section rows
    listing_rows = _row("Asking Price", ask_price)
    listing_rows += _row("Price Per SF", str(listing.get("price_per_sf") or "N/A") + "/SF")
    listing_rows += _row("Stated Cap Rate", cap_rate)
    listing_rows += _row("Building SF", str(bldg_sf) + " SF")
    listing_rows += _row("Broker", broker)
    dom_cls = "yellow" if isinstance(dom, int) and dom >= 30 else ""
    listing_rows += _row("Days on Market", str(dom), dom_cls)
    if listing.get("price_reduced"):
        amt = _fmt(listing.get("price_reduction_amount", 0))
        listing_rows += _row("Price Reduction", "Reduced " + amt, "red")

    # Parcel rows
    delinq_txt = "⚠ DELINQUENT — verify with DCAD" if delinquent else "Current — No Delinquency"
    delinq_cls = "red" if delinquent else "green"
    parcel_rows = _row("APN / Account", apn)
    parcel_rows += _row("Owner of Record", owner)
    parcel_rows += _row("Owner Mailing", owner_mail)
    parcel_rows += _row("Legal Description", legal_desc)
    parcel_rows += _row("Year Built", year_built)
    parcel_rows += _row("Assessed — Land", assessed_land)
    parcel_rows += _row("Assessed — Improvement", assessed_imp)
    parcel_rows += _row("Total Assessed Value", assessed_total)
    parcel_rows += _row("Tax Delinquency", delinq_txt, delinq_cls)

    # Flood rows
    flood_zone_cls = "green" if flood_zone == "X" else ("red" if flood_zone in ("AE","A","VE") else "yellow")
    flood_ins_txt = "YES — required by lender" if flood_ins_req else "No (Zone X)"
    flood_ins_cls = "red" if flood_ins_req else "green"
    flood_rows = _row("Flood Zone", flood_zone + " — " + flood_desc, flood_zone_cls)
    flood_rows += _row("FIRM Panel", flood_panel)
    flood_rows += _row("Effective Date", flood_date)
    flood_rows += _row("Flood Insurance Required", flood_ins_txt, flood_ins_cls)

    # Demo rows
    demo_rows = _row("Population", pop)
    demo_rows += _row("Median Household Income", mhi)
    demo_rows += _row("Owner-Occupied Housing", own_pct)
    demo_rows += _row("Median Age", med_age)
    unemp = demo.get("unemployment_rate")
    if unemp:
        demo_rows += _row("Unemployment Rate", str(unemp) + "%")

    # Business table rows
    biz_rows_html = ""
    for b in biz_list:
        flag  = b.get("status_flag", "yellow")
        col   = _color(flag)
        name  = b.get("name") or "No registration found"
        status = b.get("status", "Unknown")
        filed  = b.get("file_date") or "—"
        suite  = b.get("suite", "—")
        biz_rows_html += (
            '<tr>'
            f'<td>{suite}</td>'
            f'<td style="color:{col};font-weight:600">{name}</td>'
            f'<td style="color:{col}">{status}</td>'
            f'<td>{filed}</td>'
            '</tr>'
        )

    # Business section
    if biz_list:
        biz_section = (
            '<div class="section">'
            '<div class="sec-head">'
            '<div class="sec-title">🏪 Business Registrations at Address</div>'
            '<div class="sec-src">Source: <a href="https://mycpa.cpa.state.tx.us/coa/Index.do" target="_blank">Texas Comptroller / TX SOS</a></div>'
            '</div>'
            '<div class="sec-body">'
            '<table class="dt">'
            '<thead><tr><th>Suite</th><th>Business Name</th><th>Status</th><th>Since</th></tr></thead>'
            '<tbody>' + biz_rows_html + '</tbody>'
            '</table>'
            '<p style="color:#64748b;font-size:0.72rem;margin-top:10px">Active registration does not confirm current occupancy. Field verify.</p>'
            '</div></div>'
        )
    else:
        biz_section = ""

    # Flags section
    flag_items = ""
    for f in flags:
        ft = f.get("type", "yellow")
        flag_items += (
            '<div style="background:' + _bg(ft) + ';border:1px solid ' + _border(ft) + ';'
            'border-radius:8px;padding:12px 16px;font-size:0.85rem;color:#94a3b8">'
            '<strong style="color:' + _color(ft) + '">' + f.get("text", "") + '</strong></div>'
        )
    if flags:
        flags_section = (
            '<div class="section">'
            '<div class="sec-head">'
            '<div class="sec-title">🚦 Key Flags</div>'
            '<div class="sec-src">Compiled from verified sources above</div>'
            '</div>'
            '<div class="sec-body"><div class="flags">' + flag_items + '</div></div>'
            '</div>'
        )
    else:
        flags_section = ""

    # Pro sections
    pro_html = ""
    if is_pro:
        # Motivation score
        mot_score = motivation.get("score", 0)
        mot_tier  = motivation.get("tier", "")
        mot_text  = motivation.get("interpretation", "")
        mot_col   = "#f59e0b" if mot_score >= 40 else "#22c55e"
        mot_pct   = min(mot_score, 100)

        mot_rows_html = ""
        for ind in motivation.get("indicators", []):
            pts = ind.get("points", 0)
            pts_col = "#f59e0b" if pts > 0 else "#64748b"
            pts_str = ("+" if pts > 0 else "") + str(pts)
            mot_rows_html += (
                '<tr>'
                '<td><strong>' + ind.get("name", "") + '</strong><br>'
                '<span style="font-size:0.78rem;color:#94a3b8">' + ind.get("evidence", "") + '</span></td>'
                '<td style="color:#94a3b8;font-size:0.78rem">' + ind.get("source", "") + '</td>'
                '<td style="color:' + pts_col + ';font-weight:800;font-size:1rem">' + pts_str + '</td>'
                '</tr>'
            )

        pro_html += (
            '<div class="section">'
            '<div class="sec-head">'
            '<div class="sec-title">🎯 Motivation Score <span class="pro-tag">PRO</span></div>'
            '<div class="sec-src">Calculated from verified data only — no subjective inputs</div>'
            '</div>'
            '<div class="sec-body">'
            '<div style="display:flex;align-items:center;gap:24px;margin-bottom:20px;flex-wrap:wrap">'
            '<div style="text-align:center">'
            '<div style="font-size:3rem;font-weight:900;color:' + mot_col + ';line-height:1">' + str(mot_score) + '</div>'
            '<div style="font-size:0.72rem;color:#64748b;font-weight:700;text-transform:uppercase;letter-spacing:1px">/ 100 · ' + mot_tier + '</div>'
            '</div>'
            '<div style="flex:1">'
            '<div style="height:10px;background:#2e2e2e;border-radius:100px;overflow:hidden;margin-bottom:8px">'
            '<div style="width:' + str(mot_pct) + '%;height:100%;background:linear-gradient(90deg,#22c55e,' + mot_col + ');border-radius:100px"></div>'
            '</div>'
            '<div style="font-size:0.88rem;color:#94a3b8">' + mot_text + '</div>'
            '</div></div>'
            '<table class="dt"><thead><tr><th>Indicator</th><th>Source</th><th>Points</th></tr></thead>'
            '<tbody>' + mot_rows_html + '</tbody></table>'
            '<p style="color:#64748b;font-size:0.72rem;margin-top:12px">Score derived exclusively from verified public record data.</p>'
            '</div></div>'
        )

        # Owner intel
        skip_trace = data.get("skip_trace", {})
        skip_note  = skip_trace.get("note", "Add DataZapp API key to .env to enable")
        pro_html += (
            '<div class="section">'
            '<div class="sec-head">'
            '<div class="sec-title">👤 Owner Intelligence <span class="pro-tag">PRO</span></div>'
            '<div class="sec-src">TX SOS · DataZapp Skip Trace · DCAD</div>'
            '</div>'
            '<div class="sec-body">'
            + _row("Owner of Record", owner)
            + _row("Owner Mailing", owner_mail)
            + _row("Skip Trace Status", skip_note, "yellow")
            + '<div style="background:rgba(59,130,246,0.08);border:1px solid rgba(59,130,246,0.2);'
            'border-radius:8px;padding:14px;margin-top:12px;font-size:0.82rem;color:#94a3b8">'
            '💡 <strong style="color:#60a5fa">Enable live skip trace:</strong> Add '
            '<code style="background:#2e2e2e;padding:2px 6px;border-radius:4px">DATAZAPP_API_KEY</code> '
            'to <code style="background:#2e2e2e;padding:2px 6px;border-radius:4px">backend/.env</code>. '
            'Cost: ~$0.03/trace. Sign up at <a href="https://www.datazapp.com" style="color:#60a5fa">datazapp.com</a>.'
            '</div>'
            '</div></div>'
        )

    # Header tags
    tag_html = '<span class="tag">ZIP ' + zip_code + '</span>'
    if bldg_sf and bldg_sf != "N/A":
        tag_html += '<span class="tag">' + str(bldg_sf) + ' SF</span>'
    if cap_rate and cap_rate != "N/A":
        tag_html += '<span class="tag">Cap ' + cap_rate + '</span>'
    tag_html += '<span class="tag">Flood ' + flood_zone + '</span>'

    html = (
        '<!DOCTYPE html><html lang="en"><head>'
        '<meta charset="UTF-8"/>'
        '<meta name="viewport" content="width=device-width, initial-scale=1.0"/>'
        '<title>PropIntel Report — ' + address + '</title>'
        '<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800;900&display=swap" rel="stylesheet"/>'
        '<style>' + CSS + '</style>'
        '</head><body>'

        '<div class="topbar">'
        '<a href="/" class="logo"><div class="logo-dot">🔍</div>PropIntel' + pro_badge + '</a>'
        '<div class="tbar-right">'
        '<a href="/">← Back</a>'
        '<button class="print-btn" onclick="window.print()">⬇ Save PDF</button>'
        '</div></div>'

        '<div class="report">'

        '<div class="rhead">'
        '<div class="tier-badge">' + tier_label + '</div>'
        '<div class="prop-name">' + address + '</div>'
        '<div class="prop-addr">' + county + ' · APN: ' + apn + ' · Generated ' + ts + '</div>'
        '<div class="tags">' + tag_html + '</div>'
        '</div>'

        '<div class="section">'
        '<div class="sec-head">'
        '<div class="sec-title">📋 Listing Facts</div>'
        '<div class="sec-src">Source: <a href="' + listing_url + '" target="_blank">' + source_site + '</a></div>'
        '</div><div class="sec-body">' + listing_rows + '</div></div>'

        '<div class="section">'
        '<div class="sec-head">'
        '<div class="sec-title">🏛 Parcel &amp; Tax Record</div>'
        '<div class="sec-src">Source: <a href="https://www.dcad.org/property-search/" target="_blank">Dallas Central Appraisal District</a></div>'
        '</div><div class="sec-body">' + parcel_rows + '</div></div>'

        '<div class="section">'
        '<div class="sec-head">'
        '<div class="sec-title">🌊 FEMA Flood Zone</div>'
        '<div class="sec-src">Source: <a href="' + flood_src_url + '" target="_blank">FEMA NFHL REST API</a></div>'
        '</div><div class="sec-body">' + flood_rows + '</div></div>'

        '<div class="section">'
        '<div class="sec-head">'
        '<div class="sec-title">👥 Demographics — ZIP ' + zip_code + '</div>'
        '<div class="sec-src">Source: <a href="' + demo_src + '" target="_blank">Census ACS 5-Year Estimates</a></div>'
        '</div><div class="sec-body">' + demo_rows + '</div></div>'

        + biz_section
        + flags_section
        + pro_html

        + '<div class="disc"><div class="disc-t">Report Limitations</div>'
        '<p>This report contains only verified public record data. It does not include verified rent roll, actual NOI, '
        'DSCR, cap rate validation, financial projections, Phase I ESA, or full title search. '
        'PropIntel is not a licensed broker, appraiser, or attorney. This report does not constitute investment advice. '
        'Verify all data with licensed professionals before making investment decisions.</p></div>'

        '</div></body></html>'
    )

    return html


def generate_pdf(html: str, output_path: str) -> str:
    try:
        from weasyprint import HTML as WH
        WH(string=html).write_pdf(output_path)
        return output_path
    except ImportError:
        html_path = output_path.replace(".pdf", ".html")
        with open(html_path, "w") as f:
            f.write(html)
        return html_path
    except Exception as e:
        raise RuntimeError(f"PDF generation failed: {e}")
