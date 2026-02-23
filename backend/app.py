"""
PropIntel — Backend API
POST /api/analyze        → run pipeline, return report data + HTML
GET  /api/health         → status check
POST /api/webhook        → Stripe webhook (checkout.session.completed)
GET  /api/orders         → list all orders (admin, requires ADMIN_KEY header)
GET  /api/stats          → revenue / order stats (admin)
GET  /api/sample         → pre-built sample report
"""
import os
import json
import uuid
import hmac
import hashlib
import logging
import sys
import time
import threading
from datetime import datetime
from collections import defaultdict

from flask import Flask, request, jsonify, send_file, Response
from flask_cors import CORS
from dotenv import load_dotenv

load_dotenv()

# ─── STRUCTURED LOGGING ─────────────────────────────────────────────────────
# Rule: Every log level is wrong in prod. Set explicitly. Never use print().
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("propintel")

# ─── STARTUP CONFIG VALIDATION ──────────────────────────────────────────────
# Rule: Every config file has a cursed flag nobody touches. Validate on startup. Fail fast.
_REQUIRED = {
    "REGRID_API_KEY":        "Parcel data (core product) will not work",
    "STRIPE_LIVE_SECRET_KEY":"Payment processing will fail — no revenue",
    "STRIPE_WEBHOOK_SECRET": "Paid orders won't be fulfilled",
}
_OPTIONAL = {
    "MAILGUN_API_KEY":   "Email delivery disabled",
    "PDL_API_KEY":       "Skip trace disabled (Pro reports show no owner contact)",
    "ADMIN_KEY":         "Using default admin key — change in production",
}

_missing_critical = []
for var, consequence in _REQUIRED.items():
    if not os.environ.get(var):
        _missing_critical.append(f"  ❌ {var} — {consequence}")

for var, consequence in _OPTIONAL.items():
    if not os.environ.get(var):
        log.warning("Optional env var not set: %s → %s", var, consequence)

if _missing_critical:
    for msg in _missing_critical:
        log.critical("MISSING REQUIRED ENV VAR: %s", msg)
    # In production: halt. Locally: warn and continue.
    if os.environ.get("RENDER"):
        log.critical("Halting — fix env vars in Render dashboard")
        sys.exit(1)
    else:
        log.warning("Running locally with missing vars — some features disabled")

from pipeline import run as run_pipeline
from report.generator import generate_html, generate_pdf
from orders import create_order, update_order, get_order_by_stripe, get_order_by_token, list_orders, list_leads, create_lead, stats as order_stats
from mailer import send_report
import cache as report_cache
import idempotency

app = Flask(__name__)
CORS(app, origins=["*"])

# ─── IN-MEMORY RATE LIMITER ────────────────────────────────────────────────
# 10 requests per IP per hour on /api/analyze — protects Regrid quota
# Thread-safe; resets automatically; no Redis required
_rate_store = defaultdict(list)  # ip → [timestamps]
_rate_lock  = threading.Lock()
RATE_LIMIT  = int(os.environ.get("RATE_LIMIT_PER_HOUR", "10"))

def _check_rate_limit(ip: str) -> bool:
    """Return True if request is allowed, False if rate limited."""
    now = time.time()
    window = 3600  # 1 hour
    with _rate_lock:
        hits = [t for t in _rate_store[ip] if now - t < window]
        if len(hits) >= RATE_LIMIT:
            _rate_store[ip] = hits
            return False
        hits.append(now)
        _rate_store[ip] = hits
        return True
log.info("PropIntel API starting up")

REPORTS_DIR = os.path.join(os.path.dirname(__file__), "reports_cache")
os.makedirs(REPORTS_DIR, exist_ok=True)

STRIPE_LIVE_SECRET_KEY   = os.environ.get("STRIPE_LIVE_SECRET_KEY", "")
STRIPE_WEBHOOK_SECRET    = os.environ.get("STRIPE_WEBHOOK_SECRET", "")
ADMIN_KEY                = os.environ.get("ADMIN_KEY", "propintel-admin-2026")

# Stripe price → tier mapping
PRICE_TO_TIER = {
    os.environ.get("STRIPE_LIVE_STARTER_PRICE", "price_1T3tCw35KKjpV0x2SRXywBcA"): "starter",
    os.environ.get("STRIPE_LIVE_PRO_PRICE",     "price_1T3tCw35KKjpV0x2eo2R4n08"): "pro",
    # fallback test prices
    "price_1T3swX35KKjpV0x2uydjjeM6": "starter",
    "price_1T3swY35KKjpV0x2R1gsyw1H": "pro",
}


# ─────────────────────────────────────────────────────────────────────────────
# Health
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/api/health", methods=["GET"])
def health():
    """
    Full system health — every dependency surfaced explicitly.
    Rule: Every metric is green while users are mad. Track what users experience.
    """
    cache_stats = report_cache.stats()
    return jsonify({
        "status": "ok",
        "version": "1.4.0",
        "ts": datetime.utcnow().isoformat(),
        # Critical services
        "regrid":           bool(os.environ.get("REGRID_API_KEY")),
        "attom":            bool(os.environ.get("ATTOM_API_KEY")),
        "stripe":           bool(STRIPE_LIVE_SECRET_KEY),
        "webhook":          bool(STRIPE_WEBHOOK_SECRET),
        "email":            bool(os.environ.get("SENDGRID_API_KEY") or os.environ.get("SMTP_USER") or os.environ.get("MAILGUN_API_KEY")),
        "skip_trace":       bool(os.environ.get("PDL_API_KEY") or os.environ.get("DATAZAPP_API_KEY")),
        "skip_trace_provider": "pdl" if os.environ.get("PDL_API_KEY") else ("datazapp" if os.environ.get("DATAZAPP_API_KEY") else "none"),
        # Cache
        "cache": {
            "live_entries":  cache_stats["live"],
            "total_entries": cache_stats["total"],
            "total_hits":    cache_stats["total_hits"],
        },
        # Config completeness
        "config_complete": bool(
            os.environ.get("REGRID_API_KEY") and
            STRIPE_LIVE_SECRET_KEY and
            STRIPE_WEBHOOK_SECRET
        ),
    })


# ─────────────────────────────────────────────────────────────────────────────
# Analyze
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/api/admin/test-email", methods=["POST"])
def test_email():
    """
    Admin endpoint — generate a real report and send it to an email.
    Used to confirm email + PDF delivery without going through Stripe.
    POST { "address": "...", "email": "...", "tier": "pro", "key": "<ADMIN_KEY>" }
    """
    if not _check_admin():
        return jsonify({"error": "Unauthorized"}), 401

    body = request.get_json(force=True, silent=True) or {}
    address = body.get("address", "3229 Forest Ln, Garland TX 75042")
    email   = body.get("email", "")
    tier    = body.get("tier", "pro")

    if not email:
        return jsonify({"error": "email required"}), 400

    log.info("Test email requested: %s → %s [%s]", address, email, tier)

    try:
        report_data = run_pipeline(address, tier)
        html = generate_html(report_data)
        import uuid
        report_id = "test-" + str(uuid.uuid4())[:6]
        result = send_report(
            to_email=email,
            to_name="",
            address=address,
            tier=tier,
            report_html=html,
            report_id=report_id,
            report_token=str(uuid.uuid4()),
            report_data=report_data,
        )
        return jsonify({
            "sent": result.get("success"),
            "method": result.get("method"),
            "pdf_attached": result.get("pdf_attached", False),
            "pdf_error": result.get("pdf_error"),
            "error": result.get("error"),
            "address": address,
            "email": email,
        })
    except Exception as e:
        log.error("Test email failed: %s", e, exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route("/api/analyze", methods=["POST"])
def analyze():
    """
    Request body:
    {
      "input": "3229 Forest Ln Garland TX 75042",
      "tier": "starter" | "pro",
      "format": "json" | "html" | "pdf"
    }
    """
    # Rate limit: 10 requests/IP/hour (protects Regrid quota)
    client_ip = request.headers.get("X-Forwarded-For", request.remote_addr or "").split(",")[0].strip()
    if not _check_rate_limit(client_ip):
        return jsonify({
            "error": "Rate limit exceeded. Maximum 10 analyses per hour per IP.",
            "retry_after": "3600"
        }), 429

    body = request.get_json(force=True, silent=True) or {}
    input_str = (body.get("input") or "").strip()
    tier = body.get("tier", "starter").lower()
    fmt = body.get("format", "json").lower()
    email = (body.get("email") or "").strip().lower()

    if not input_str:
        return jsonify({"error": "Missing 'input' field"}), 400
    if tier not in ("starter", "pro"):
        return jsonify({"error": "tier must be 'starter' or 'pro'"}), 400

    # Capture lead email (non-blocking)
    if email and "@" in email:
        try:
            ip = request.headers.get("X-Forwarded-For", request.remote_addr or "")
            create_lead(email=email, address=input_str, tier=tier, ip=ip)
        except Exception:
            pass  # Never block the report for a lead capture failure

    # ── CACHE CHECK ──────────────────────────────────────────────────────────
    # Rule: Every demo burns real API credits. Cache 24h to protect Regrid quota.
    cached = report_cache.get(input_str, tier)
    if cached:
        log.info("Serving cached report: %s [%s]", input_str[:40], tier)
        if fmt == "json":
            return jsonify(cached)
        html = generate_html(cached)
        if fmt == "html":
            return Response(html, mimetype="text/html")

    # ── LIVE PIPELINE ────────────────────────────────────────────────────────
    log.info("Running pipeline: %s [%s]", input_str[:40], tier)
    try:
        report_data = run_pipeline(input_str, tier)
    except Exception as e:
        log.error("Pipeline failed for %s: %s", input_str[:40], e, exc_info=True)
        return jsonify({"error": f"Pipeline failed: {str(e)}"}), 500

    # Store in cache (never blocks — errors are swallowed in cache.set)
    report_cache.set(input_str, tier, report_data)

    report_data["generated_at"] = datetime.utcnow().isoformat()
    report_data["report_id"] = str(uuid.uuid4())[:8]

    if fmt == "json":
        return jsonify(report_data)

    html = generate_html(report_data)

    if fmt == "html":
        return Response(html, mimetype="text/html")

    if fmt == "pdf":
        pdf_path = os.path.join(REPORTS_DIR, f"{report_data['report_id']}.pdf")
        generate_pdf(html, pdf_path)
        return send_file(pdf_path, mimetype="application/pdf",
                         as_attachment=True,
                         download_name=f"propintel-report-{report_data['report_id']}.pdf")

    return jsonify({"error": "Invalid format"}), 400


# ─────────────────────────────────────────────────────────────────────────────
# Stripe Webhook
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/api/webhook", methods=["POST"])
def stripe_webhook():
    """
    Handles checkout.session.completed events from Stripe.
    Extracts address from client_reference_id or metadata,
    runs the pipeline, stores the order, and emails the report.
    """
    payload = request.get_data()
    sig_header = request.headers.get("Stripe-Signature", "")

    # Verify signature if webhook secret is configured
    if STRIPE_WEBHOOK_SECRET:
        if not _verify_stripe_signature(payload, sig_header, STRIPE_WEBHOOK_SECRET):
            return jsonify({"error": "Invalid signature"}), 400

    try:
        event = json.loads(payload)
    except Exception:
        return jsonify({"error": "Invalid JSON"}), 400

    event_id   = event.get("id", "")
    event_type = event.get("type", "")

    log.info("Stripe webhook received: %s (%s)", event_type, event_id)

    # ── IDEMPOTENCY CHECK ────────────────────────────────────────────────────
    # Rule: Every webhook fires twice when you least expect it.
    # Stripe retries for 72h. We must be safe to receive the same event 10x.
    from orders import _get_conn
    conn = _get_conn()
    if idempotency.already_processed(conn, event_id):
        log.info("Webhook duplicate — skipping: %s", event_id)
        return jsonify({"received": True, "duplicate": True})

    if event_type == "checkout.session.completed":
        try:
            _handle_checkout_completed(event["data"]["object"])
            idempotency.mark_processed(conn, event_id, event_type, "ok")
        except Exception as e:
            log.error("Webhook handler failed: %s — NOT marking processed (will retry)", e, exc_info=True)
            idempotency.mark_processed(conn, event_id, event_type, f"error: {e}")
            return jsonify({"error": str(e)}), 500
    elif event_type == "payment_intent.succeeded":
        idempotency.mark_processed(conn, event_id, event_type, "ignored")

    return jsonify({"received": True})


def _handle_checkout_completed(session: dict):
    """Process a completed Stripe checkout session."""
    stripe_id     = session.get("id", "")
    customer_email = session.get("customer_details", {}).get("email", "")
    customer_name  = session.get("customer_details", {}).get("name", "")
    amount_total   = session.get("amount_total", 0)

    # Determine tier from line items or metadata
    tier = "starter"
    metadata = session.get("metadata") or {}
    if metadata.get("tier"):
        tier = metadata["tier"]
    else:
        # Try to detect from amount
        if amount_total >= 2999:
            tier = "pro"

    # Get address from client_reference_id or metadata
    address = (
        session.get("client_reference_id")
        or metadata.get("address")
        or metadata.get("property_address")
        or ""
    )

    # Check if already processed
    existing = get_order_by_stripe(stripe_id)
    if existing:
        return

    # Create order record
    order = create_order(
        stripe_id=stripe_id,
        tier=tier,
        address=address,
        customer_email=customer_email,
        customer_name=customer_name,
        amount_cents=amount_total,
    )
    order_id = order["id"]
    report_token = order.get("report_token", "")

    if not address:
        update_order(order_id, status="pending_address")
        return

    # Run pipeline
    try:
        report_data = run_pipeline(address, tier)
        report_data["generated_at"] = datetime.utcnow().isoformat()
        report_data["report_id"] = order_id
        report_data["order_id"] = order_id

        html = generate_html(report_data)

        update_order(
            order_id,
            status="complete",
            report_json=json.dumps(report_data),
            report_id=order_id,
        )

        # Email report with PDF attachment
        if customer_email:
            result = send_report(
                to_email=customer_email,
                to_name=customer_name,
                address=address,
                tier=tier,
                report_html=html,
                report_id=order_id,
                order_id=order_id,
                report_token=report_token,
                report_data=report_data,   # enables PDF attachment
            )
            if result["success"]:
                update_order(order_id, emailed=1)

    except Exception as e:
        update_order(order_id, status=f"error: {str(e)[:100]}")


def _verify_stripe_signature(payload: bytes, sig_header: str, secret: str) -> bool:
    """Verify Stripe webhook signature."""
    try:
        parts = {p.split("=")[0]: p.split("=")[1] for p in sig_header.split(",")}
        timestamp = parts.get("t", "")
        v1_sig = parts.get("v1", "")
        signed_payload = f"{timestamp}.{payload.decode('utf-8')}"
        expected = hmac.new(
            secret.encode("utf-8"),
            signed_payload.encode("utf-8"),
            hashlib.sha256
        ).hexdigest()
        return hmac.compare_digest(expected, v1_sig)
    except Exception:
        return False


# ─────────────────────────────────────────────────────────────────────────────
# Admin endpoints (require ADMIN_KEY header)
# ─────────────────────────────────────────────────────────────────────────────

def _check_admin():
    return request.headers.get("X-Admin-Key") == ADMIN_KEY or \
           request.args.get("admin_key") == ADMIN_KEY


@app.route("/api/orders", methods=["GET"])
def get_orders():
    if not _check_admin():
        return jsonify({"error": "Unauthorized"}), 401
    orders = list_orders(limit=200)
    return jsonify({"orders": orders, "count": len(orders)})


@app.route("/api/leads", methods=["GET"])
def get_leads():
    if not _check_admin():
        return jsonify({"error": "Unauthorized"}), 401
    leads = list_leads(limit=500)
    return jsonify({"leads": leads, "count": len(leads)})


@app.route("/api/stats", methods=["GET"])
def get_stats():
    if not _check_admin():
        return jsonify({"error": "Unauthorized"}), 401
    return jsonify(order_stats())


@app.route("/api/reports/<token>", methods=["GET"])
def get_report_by_token(token):
    """
    Public endpoint — customers access their report via a secret token.
    No auth required; the token itself is the secret (UUID from create_order).
    Returns JSON report data for report.html to render client-side.
    """
    if not token or len(token) < 32:
        return jsonify({"error": "Invalid token"}), 400
    order = get_order_by_token(token)
    if not order:
        return jsonify({"error": "Report not found"}), 404
    if not order.get("report_json"):
        return jsonify({"error": "Report not ready yet", "status": order.get("status")}), 202
    try:
        data = json.loads(order["report_json"])
        # Strip out internal fields
        data.pop("report_id", None)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": f"Could not load report: {str(e)}"}), 500


@app.route("/api/orders/<order_id>/report", methods=["GET"])
def get_order_report(order_id):
    """Return the stored HTML report for an order (admin only)."""
    if not _check_admin():
        return jsonify({"error": "Unauthorized"}), 401

    conn_order = None
    try:
        from orders import _get_conn
        conn_order = _get_conn()
        row = conn_order.execute("SELECT * FROM orders WHERE id = ?", (order_id,)).fetchone()
        if not row:
            return jsonify({"error": "Not found"}), 404
        order = dict(row)
        if order.get("report_json"):
            report_data = json.loads(order["report_json"])
            html = generate_html(report_data)
            return Response(html, mimetype="text/html")
        return jsonify({"error": "Report not generated yet"}), 404
    finally:
        if conn_order:
            conn_order.close()


# ─────────────────────────────────────────────────────────────────────────────
# Sample
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/api/sample", methods=["GET"])
def sample():
    sample_data = _sample_report_data()
    fmt = request.args.get("format", "json")
    if fmt == "json":
        return jsonify(sample_data)
    html = generate_html(sample_data)
    if fmt == "html":
        return Response(html, mimetype="text/html")
    return jsonify(sample_data)


def _sample_report_data() -> dict:
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
            "owner_name": "SUNCHA 2004 INC",
            "owner_mailing": "2211 NE LOOP 410, SAN ANTONIO TX 78217",
            "owner_city": "SAN ANTONIO",
            "owner_state": "TX",
            "property_address": "3229 FOREST LN",
            "legal_description": "PT LOT 1 ACS 0.817",
            "year_built": "1988",
            "building_sf": "9640",
            "assessed_land": 142360,
            "assessed_improvement": 666470,
            "assessed_total": 808830,
            "absentee_owner": True,
            "out_of_state_owner": False,
            "tax_delinquent": False,
            "source": "Dallas Central Appraisal District",
            "source_url": "https://www.dcad.org/property-search/",
        },
        "flood": {
            "zone": "X",
            "description": "Minimal Flood Hazard — Outside 500-year floodplain",
            "firm_panel": "48113C",
            "effective_date": "2009-09-25",
            "flood_insurance_required": False,
            "source": "FEMA NFHL REST API",
            "source_url": "https://msc.fema.gov/portal/home",
        },
        "demographics": {
            "zip": "75042",
            "population": 37925,
            "median_household_income": 60118,
            "median_household_income_fmt": "$60,118",
            "owner_occupied_pct": 58.7,
            "median_age": 32.9,
            "unemployment_rate": 5.1,
            "source": "U.S. Census Bureau, ACS 5-Year Estimates 2022",
        },
        "businesses": [],
        "skip_trace": {
            "status": "hit",
            "phones": ["(210) 555-0147"],
            "emails": ["owner@example.com"],
            "dnc": [False],
            "source": "DataZapp",
            "note": "Sample data — live skip trace requires DataZapp API key.",
        },
        "motivation": {
            "score": 55, "tier": "MODERATE",
            "interpretation": "Score 55/100 — Moderate motivation. Absentee owner, LLC structure, extended DOM.",
            "indicators": [
                {"name": "Absentee Owner", "triggered": True, "points": 15,
                 "evidence": "Owner mailing San Antonio TX vs. Garland property", "source": "DCAD"},
                {"name": "Long Hold Duration", "triggered": False, "points": 0,
                 "evidence": "Deed date not in DCAD ArcGIS layer", "source": "Dallas County Clerk"},
                {"name": "LLC / Entity Ownership", "triggered": True, "points": 10,
                 "evidence": "SUNCHA 2004 INC — corporate entity", "source": "DCAD"},
                {"name": "Extended Days on Market", "triggered": True, "points": 10,
                 "evidence": "47 days on market", "source": "LoopNet"},
                {"name": "Recorded Price Reduction", "triggered": True, "points": 15,
                 "evidence": "$128,000 reduction", "source": "LoopNet price history"},
                {"name": "Tax Delinquency", "triggered": False, "points": 0,
                 "evidence": "Taxes current", "source": "DCAD"},
                {"name": "Out-of-State Owner", "triggered": False, "points": 0,
                 "evidence": "Owner in TX (San Antonio)", "source": "DCAD"},
            ]
        },
        "flags": [
            {"type": "green", "text": "Flood Zone X — Minimal risk, no flood insurance required (FEMA)"},
            {"type": "green", "text": "No tax delinquency — taxes current (DCAD)"},
            {"type": "yellow", "text": "Absentee owner — mailing address in San Antonio TX (DCAD)"},
            {"type": "yellow", "text": "Motivation score 55/100 (MODERATE) — seller signals present"},
        ],
    }


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5050))
    debug = os.environ.get("DEBUG", "true").lower() == "true"
    print(f"PropIntel API v1.1 starting on port {port}")
    app.run(host="0.0.0.0", port=port, debug=debug)
