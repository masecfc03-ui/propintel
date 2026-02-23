"""
PropIntel — Backend API
POST /api/analyze → run pipeline, return report data + HTML
GET  /api/health  → status check
"""
import os
import json
import uuid
from datetime import datetime
from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
from dotenv import load_dotenv

load_dotenv()

from pipeline import run as run_pipeline
from report.generator import generate_html, generate_pdf

app = Flask(__name__)
CORS(app, origins=["*"])  # Restrict in production

REPORTS_DIR = os.path.join(os.path.dirname(__file__), "reports_cache")
os.makedirs(REPORTS_DIR, exist_ok=True)


@app.route("/api/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "version": "1.0.0", "ts": datetime.utcnow().isoformat()})


@app.route("/api/analyze", methods=["POST"])
def analyze():
    """
    Request body:
    {
      "input": "3229 Forest Ln Garland TX 75042",  // address or URL
      "tier": "starter" | "pro",
      "format": "json" | "html" | "pdf"            // default: json
    }
    """
    body = request.get_json(force=True, silent=True) or {}
    input_str = (body.get("input") or "").strip()
    tier = body.get("tier", "starter").lower()
    fmt = body.get("format", "json").lower()

    if not input_str:
        return jsonify({"error": "Missing 'input' field"}), 400
    if tier not in ("starter", "pro"):
        return jsonify({"error": "tier must be 'starter' or 'pro'"}), 400

    # Run pipeline
    try:
        report_data = run_pipeline(input_str, tier)
    except Exception as e:
        return jsonify({"error": f"Pipeline failed: {str(e)}"}), 500

    report_data["generated_at"] = datetime.utcnow().isoformat()
    report_data["report_id"] = str(uuid.uuid4())[:8]

    if fmt == "json":
        return jsonify(report_data)

    # Generate HTML report
    html = generate_html(report_data)

    if fmt == "html":
        from flask import Response
        return Response(html, mimetype="text/html")

    if fmt == "pdf":
        pdf_path = os.path.join(REPORTS_DIR, f"{report_data['report_id']}.pdf")
        generate_pdf(html, pdf_path)
        return send_file(pdf_path, mimetype="application/pdf",
                         as_attachment=True,
                         download_name=f"deallens-report-{report_data['report_id']}.pdf")

    return jsonify({"error": "Invalid format"}), 400


@app.route("/api/sample", methods=["GET"])
def sample():
    """Return a pre-built sample report (Forest Jupiter Plaza) for demo purposes."""
    sample_data = _sample_report_data()
    fmt = request.args.get("format", "json")

    if fmt == "json":
        return jsonify(sample_data)

    html = generate_html(sample_data)
    if fmt == "html":
        from flask import Response
        return Response(html, mimetype="text/html")

    return jsonify(sample_data)


def _sample_report_data() -> dict:
    """Hard-coded sample for Forest Jupiter Plaza (Garland TX)."""
    return {
        "report_id": "SAMPLE",
        "tier": "pro",
        "input": "3229-3249 Forest Ln, Garland TX 75042",
        "input_type": "address",
        "resolved_address": "3229-3249 FOREST LN, GARLAND, TX, 75042",
        "generated_at": datetime.utcnow().isoformat(),
        "geo": {
            "lat": 32.9141, "lng": -96.6389,
            "zip": "75042", "state": "TX", "county": "Dallas County", "city": "Garland"
        },
        "listing": {
            "asking_price": 2578000,
            "asking_price_fmt": "$2,578,000",
            "price_per_sf": 267.43,
            "cap_rate": "7.22%",
            "building_sf": "9640",
            "property_type": "Strip Center",
            "broker": "United Shinryu Brokerage LLC",
            "listing_id": "32972360",
            "days_on_market": 47,
            "price_reduced": True,
            "price_reduction_amount": 128000,
            "source_site": "loopnet",
            "url": "https://www.loopnet.com/Listing/3229-3249-Forest-Ln-Garland-TX/32972360/",
        },
        "parcel": {
            "apn": "26629500010010700",
            "owner_name": "FOREST JUPITER PROPERTIES LLC",
            "owner_mailing": "PO BOX 12345, DALLAS TX 75201",
            "property_address": "3229-3249 FOREST LN GARLAND TX 75042",
            "legal_description": "LOT 1, BLK A, FOREST JUPITER ADDITION",
            "year_built": "1988",
            "building_sf": "9640",
            "assessed_land": 412000,
            "assessed_improvement": 1847000,
            "assessed_total": 2259000,
            "taxable_value": 2259000,
            "tax_delinquent": False,
            "source": "Dallas Central Appraisal District",
            "source_url": "https://www.dcad.org/property-search/",
        },
        "flood": {
            "zone": "X",
            "description": "Minimal Flood Hazard — Outside 500-year floodplain",
            "firm_panel": "48113C0285J",
            "effective_date": "2009-09-25",
            "flood_insurance_required": False,
            "source": "FEMA NFHL REST API",
            "source_url": "https://msc.fema.gov/portal/home",
        },
        "demographics": {
            "zip": "75042",
            "population": 39148,
            "median_household_income": 64266,
            "median_household_income_fmt": "$64,266",
            "owner_occupied_pct": 51.3,
            "median_age": 32.4,
            "source": "U.S. Census Bureau, ACS 5-Year Estimates 2022",
        },
        "businesses": [
            {"name": "JADE NAILS & SPA LLC", "status": "Active", "status_flag": "green", "file_date": "04/2019"},
            {"name": "GOLDEN STAR RESTAURANT LLC", "status": "Active", "status_flag": "green", "file_date": "11/2017"},
            {"name": "FAMILY DENTAL OF GARLAND PC", "status": "Active", "status_flag": "green", "file_date": "01/2015"},
            {"name": None, "status": "No registration found", "status_flag": "yellow", "file_date": None, "suite": "3239"},
            {"name": "METRO TAX SERVICES LLC", "status": "Forfeited — 2023", "status_flag": "red", "file_date": "03/2016"},
            {"name": "GREAT CLIPS INC (FRANCHISEE)", "status": "Active", "status_flag": "green", "file_date": "08/2012"},
        ],
        "motivation": {
            "score": 70, "tier": "HIGH",
            "interpretation": "Score 70/100 — High motivation. Seller likely open to below-ask offers. Recommend direct outreach before formal broker submission.",
            "indicators": [
                {"name": "Absentee Owner", "triggered": True, "points": 15, "evidence": "Owner mailing PO BOX Dallas TX vs. Garland property", "source": "DCAD"},
                {"name": "Long Hold Duration", "triggered": True, "points": 20, "evidence": "7.9 years held (acquired March 2018)", "source": "Dallas County Clerk deed"},
                {"name": "LLC / Entity Ownership", "triggered": True, "points": 10, "evidence": "FOREST JUPITER PROPERTIES LLC", "source": "DCAD"},
                {"name": "Extended Days on Market", "triggered": True, "points": 10, "evidence": "47 days (avg 28 for strip centers)", "source": "LoopNet"},
                {"name": "Recorded Price Reduction", "triggered": True, "points": 15, "evidence": "$128,000 reduction on 01/15/2026", "source": "LoopNet price history"},
                {"name": "Tax Delinquency", "triggered": False, "points": 0, "evidence": "Taxes current", "source": "DCAD"},
                {"name": "Out-of-State Owner", "triggered": False, "points": 0, "evidence": "Owner in TX", "source": "DCAD"},
            ]
        },
        "flags": [
            {"type": "green", "text": "Flood Zone X — Minimal risk, no flood insurance required (FEMA)"},
            {"type": "green", "text": "No tax delinquency detected — taxes current (DCAD)"},
            {"type": "yellow", "text": "Motivation score 70/100 (HIGH) — seller signals present"},
            {"type": "red", "text": "1 forfeited entity found at Suite 3243 — possible vacancy (TX SOS)"},
        ],
    }


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5050))
    debug = os.environ.get("DEBUG", "true").lower() == "true"
    print(f"PropIntel API starting on port {port}")
    app.run(host="0.0.0.0", port=port, debug=debug)
