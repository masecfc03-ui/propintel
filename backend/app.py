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


@app.route("/api/admin/generate-token", methods=["POST"])
def generate_token():
    """
    Admin endpoint — run the pipeline for an address and store the report,
    returning a public token + URL without sending any email.
    Used for: agent outreach (send the URL manually), testing, white-label.
    POST { "address": "...", "tier": "pro" }
    Auth: X-Admin-Key header or ?admin_key= query param
    """
    if not _check_admin():
        return jsonify({"error": "Unauthorized"}), 401

    body    = request.get_json(force=True, silent=True) or {}
    address = body.get("address", "").strip()
    tier    = body.get("tier", "pro")

    if not address:
        return jsonify({"error": "address required"}), 400

    log.info("generate-token: %s [%s]", address, tier)

    try:
        report_data = run_pipeline(address, tier)
        report_id   = "agent-" + str(uuid.uuid4())[:8]
        token       = str(uuid.uuid4())

        # Store in DB if available
        try:
            from orders import store_report
            store_report(report_id, token, report_data)
        except Exception as db_err:
            log.warning("DB store skipped: %s", db_err)
            # Fallback: store in memory cache so token link works this session
            _report_cache[token] = {"id": report_id, "data": report_data, "ts": time.time()}

        report_url = f"{os.getenv('REPORT_BASE_URL','https://propertyvalueintel.com')}/report.html?token={token}"

        return jsonify({
            "report_id":   report_id,
            "token":       token,
            "report_url":  report_url,
            "address":     address,
            "tier":        tier,
            "motivation_score": (report_data.get("motivation") or {}).get("score"),
            "assessed_total":   (report_data.get("parcel") or {}).get("assessed_total"),
        })
    except Exception as e:
        log.error("generate-token failed: %s", e, exc_info=True)
        return jsonify({"error": str(e)}), 500


# In-memory fallback cache for tokens when DB not available
_report_cache = {}


@app.route("/api/admin/bulk-generate", methods=["POST"])
def bulk_generate():
    """
    Admin endpoint — batch report generation for up to 10 addresses.
    Useful for sending demo reports to a list of agent prospects.

    POST {
      "addresses": ["123 Main St Dallas TX", ...],  # max 10
      "tier": "pro",
      "email": "mason@example.com",   # optional — send report to this address
      "dry_run": false                # if true, run pipeline but skip email
    }
    Auth: X-Admin-Key header or ?admin_key= query param
    Returns: {"processed": N, "results": [{"address": "...", "status": "ok"|"error", "error": "..."}]}
    """
    if not _check_admin():
        return jsonify({"error": "Unauthorized"}), 401

    body = request.get_json(force=True, silent=True) or {}
    addresses = body.get("addresses", [])
    tier = body.get("tier", "pro")
    email = (body.get("email") or "").strip()
    dry_run = bool(body.get("dry_run", False))

    if not isinstance(addresses, list) or len(addresses) == 0:
        return jsonify({"error": "addresses must be a non-empty list"}), 400

    if len(addresses) > 10:
        return jsonify({"error": "Maximum 10 addresses per request"}), 400

    if tier not in ("starter", "pro"):
        return jsonify({"error": "tier must be 'starter' or 'pro'"}), 400

    should_email = bool(email and not dry_run)
    log.info(
        "bulk-generate: %d addresses, tier=%s, email=%s, dry_run=%s",
        len(addresses), tier, email or "(none)", dry_run,
    )

    results = []
    for raw_address in addresses:
        address = (raw_address or "").strip()
        if not address:
            results.append({"address": raw_address, "status": "error", "error": "empty address"})
            continue

        log.info("bulk-generate processing: %s [%s]", address, tier)
        try:
            report_data = run_pipeline(address, tier)
            report_data["generated_at"] = datetime.utcnow().isoformat()
            report_id = "bulk-" + str(uuid.uuid4())[:8]
            report_data["report_id"] = report_id

            # Store in DB if available
            report_token = str(uuid.uuid4())
            try:
                from orders import store_report
                store_report(report_id, report_token, report_data)
            except Exception as db_err:
                log.warning("bulk-generate DB store skipped for %s: %s", address, db_err)
                _report_cache[report_token] = {
                    "id": report_id,
                    "data": report_data,
                    "ts": time.time(),
                }

            entry = {"address": address, "status": "ok"}

            # Send email if requested
            if should_email:
                try:
                    html = generate_html(report_data)
                    mail_result = send_report(
                        to_email=email,
                        to_name="",
                        address=address,
                        tier=tier,
                        report_html=html,
                        report_id=report_id,
                        report_token=report_token,
                        report_data=report_data,
                    )
                    entry["emailed"] = mail_result.get("success", False)
                    entry["email_method"] = mail_result.get("method")
                    if not mail_result.get("success"):
                        entry["email_error"] = mail_result.get("error")
                except Exception as mail_err:
                    log.error("bulk-generate email failed for %s: %s", address, mail_err)
                    entry["emailed"] = False
                    entry["email_error"] = str(mail_err)

            results.append(entry)

        except Exception as e:
            log.error("bulk-generate pipeline failed for %s: %s", address, e, exc_info=True)
            results.append({"address": address, "status": "error", "error": str(e)})

    processed = sum(1 for r in results if r.get("status") == "ok")
    log.info("bulk-generate complete: %d/%d succeeded", processed, len(addresses))
    return jsonify({"processed": processed, "results": results})


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
    # Check DB first
    order = get_order_by_token(token)
    if order:
        if not order.get("report_json"):
            return jsonify({"error": "Report not ready yet", "status": order.get("status")}), 202
        try:
            data = json.loads(order["report_json"])
            data.pop("report_id", None)
            return jsonify(data)
        except Exception as e:
            return jsonify({"error": f"Could not load report: {str(e)}"}), 500

    # Fallback: check in-memory cache (used by generate-token when DB unavailable)
    cached = _report_cache.get(token)
    if cached:
        data = dict(cached.get("data", {}))
        data.pop("report_id", None)
        return jsonify(data)

    return jsonify({"error": "Report not found"}), 404


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


# ─────────────────────────────────────────────────────────────────────────────
# Agent Report Templates
# ─────────────────────────────────────────────────────────────────────────────

TEMPLATES_DIR = os.path.join(os.path.dirname(__file__), "templates")

_ALLOWED_TEMPLATES = {
    "buyer_due_diligence",
    "listing_intelligence",
    "investment_analysis",
}


def _render_agent_template(template_name: str, data: dict) -> str:
    """
    Load a template file and replace {{field}} placeholders with values from data.
    Uses simple string .replace() — no Jinja2 dependency.

    Nested data is flattened with double-underscore: data["agent"]["name"] → {{agent_name}}
    """
    template_path = os.path.join(TEMPLATES_DIR, f"{template_name}.html")
    if not os.path.exists(template_path):
        raise FileNotFoundError(f"Template not found: {template_name}")

    with open(template_path, "r", encoding="utf-8") as f:
        html = f.read()

    # Flatten nested dicts (one level deep) with underscore separator
    flat = {}
    for key, val in data.items():
        if isinstance(val, dict):
            for subkey, subval in val.items():
                flat[f"{key}_{subkey}"] = str(subval) if subval is not None else ""
        else:
            flat[key] = str(val) if val is not None else ""

    # Replace all {{field}} placeholders
    for field, value in flat.items():
        html = html.replace("{{" + field + "}}", value)

    # Clean up any remaining unreplaced placeholders (avoid leaking template syntax)
    import re as _re
    html = _re.sub(r"\{\{[^}]+\}\}", "", html)
    # Remove mustache-style conditional blocks ({{#field}}...{{/field}})
    html = _re.sub(r"\{\{[#/\^][^}]+\}\}", "", html)

    return html


def _build_template_data(report_data: dict, agent: dict) -> dict:
    """
    Assemble the template variable dict from pipeline report data + agent info.
    All values must be strings (template uses direct .replace()).
    """
    parcel       = report_data.get("parcel", {}) or {}
    market_est   = report_data.get("market_estimate", {}) or {}
    motivation   = report_data.get("motivation", {}) or {}
    flood        = report_data.get("flood", {}) or {}
    avm          = report_data.get("avm", {}) or {}
    mortgage     = report_data.get("mortgage", {}) or {}
    sold_comps   = report_data.get("sold_comps", {}) or {}
    financials   = report_data.get("financials", {}) or {}
    deal_analysis= report_data.get("deal_analysis", {}) or {}

    # ── Agent fields ──────────────────────────────────────────────────────────
    agent_name      = agent.get("name", "Your Agent")
    agent_brokerage = agent.get("brokerage", "")
    agent_phone     = agent.get("phone", "")
    agent_email     = agent.get("email", "")
    agent_logo_url  = agent.get("logo_url", "")
    # Initials fallback for logo placeholder
    parts = agent_name.split()
    agent_initials = "".join(p[0].upper() for p in parts[:2]) if parts else "AG"

    # ── Property fields ───────────────────────────────────────────────────────
    address = (
        report_data.get("resolved_address")
        or report_data.get("input", "")
    )
    beds = str(parcel.get("bedrooms") or parcel.get("total_bedrooms") or "—")
    baths = str(parcel.get("bathrooms") or parcel.get("total_bathrooms") or "—")
    building_sf = "{:,}".format(int(parcel.get("building_sf") or 0)) if parcel.get("building_sf") else "—"
    year_built = str(parcel.get("year_built") or "—")
    lot_size = str(parcel.get("lot_size_acres") or parcel.get("lot_size") or "—")
    property_type = str(report_data.get("property_class") or parcel.get("use_description") or "—").title()

    # Street View URL
    geo = report_data.get("geo", {}) or {}
    lat, lng = geo.get("lat", ""), geo.get("lng", "")
    if lat and lng:
        street_view_url = (
            f"https://maps.googleapis.com/maps/api/streetview"
            f"?size=700x220&location={lat},{lng}&fov=90&key=GOOGLE_MAPS_KEY"
        )
    else:
        # Placeholder grey image via placehold.co (no external dependency at render time)
        street_view_url = "https://placehold.co/700x220/1a2e44/ffffff?text=Property+Photo"

    # ── Valuation ──────────────────────────────────────────────────────────────
    assessed_raw = parcel.get("assessed_total") or 0
    assessed_value = market_est.get("assessed_fmt") or (f"${assessed_raw:,.0f}" if assessed_raw else "N/A")
    avm_value = market_est.get("range_fmt") or market_est.get("market_mid") or "N/A"
    if isinstance(avm_value, (int, float)):
        avm_value = f"${avm_value:,.0f}"
    avm_methodology = market_est.get("methodology") or market_est.get("confidence") or "AVM Estimate"
    avm_source = market_est.get("source") or "PropIntel AVM"
    market_low = f"${market_est.get('market_low', 0):,.0f}" if market_est.get("market_low") else "—"
    market_high = f"${market_est.get('market_high', 0):,.0f}" if market_est.get("market_high") else "—"
    comp_range_low = sold_comps.get("price_min") or market_est.get("market_low") or 0
    comp_range_high = sold_comps.get("price_max") or market_est.get("market_high") or 0
    comp_range = (
        f"${comp_range_low:,.0f} – ${comp_range_high:,.0f}"
        if comp_range_low and comp_range_high else "N/A"
    )

    # ── Ownership ──────────────────────────────────────────────────────────────
    owner_name = parcel.get("owner_name") or "N/A"
    owner_mailing = " ".join(filter(None, [
        parcel.get("owner_mailing"),
        parcel.get("owner_city"),
        parcel.get("owner_state"),
        parcel.get("owner_zip"),
    ])) or "N/A"
    hold_duration = "N/A"
    ownership_history = report_data.get("ownership_history") or {}
    if isinstance(ownership_history, dict) and ownership_history.get("hold_years"):
        hy = ownership_history["hold_years"]
        hold_duration = f"{hy:.1f} years" if isinstance(hy, float) else f"{hy} years"
    last_sale_price = "N/A"
    if isinstance(ownership_history, dict):
        hist = ownership_history.get("history", [])
        if hist and isinstance(hist[0], dict):
            sp = hist[0].get("sale_price")
            if sp:
                last_sale_price = f"${sp:,.0f}"

    # ── Motivation ──────────────────────────────────────────────────────────────
    mot_score = str(motivation.get("score", "N/A"))
    mot_tier  = motivation.get("tier", "N/A")
    mot_interp = motivation.get("interpretation") or "Insufficient data to score motivation."
    if mot_tier == "HIGH":
        motivation_bg_color   = "#c0392b"
        motivation_text_color = "#c0392b"
    elif mot_tier == "MODERATE":
        motivation_bg_color   = "#e67e22"
        motivation_text_color = "#d35400"
    else:
        motivation_bg_color   = "#27ae60"
        motivation_text_color = "#1e8449"

    # Build motivation indicators HTML block
    mot_indicators = motivation.get("indicators", [])
    mot_rows = []
    for ind in mot_indicators:
        triggered = ind.get("triggered", False)
        dot_color = "#c0392b" if triggered else "#bdc3c7"
        dot = f"<span style='display:inline-block;width:10px;height:10px;border-radius:50%;background:{dot_color};margin-right:8px;vertical-align:middle;'></span>"
        name = ind.get("name", "")
        pts  = ind.get("points", 0)
        pts_fmt = f"+{pts} pts" if triggered and pts else ""
        evidence = ind.get("evidence", "")
        row = (
            f"<tr>"
            f"<td style='padding:7px 0;border-bottom:1px solid #f0f4f8;'>"
            f"<p style='color:{'#2c3e50' if triggered else '#9aafc4'};font-size:12px;font-family:Arial,Helvetica,sans-serif;'>"
            f"{dot}<strong>{name}</strong>"
            + (f" <span style='color:#c0392b;font-size:10px;'>{pts_fmt}</span>" if pts_fmt else "")
            + (f"<br><span style='color:#9aafc4;font-size:11px;padding-left:18px;'>{evidence}</span>" if evidence else "")
            + "</p></td></tr>"
        )
        mot_rows.append(row)
    motivation_indicators_html = "\n".join(mot_rows) if mot_rows else "<tr><td><p style='color:#9aafc4;font-size:12px;font-family:Arial,Helvetica,sans-serif;'>Motivation data not available for this tier.</p></td></tr>"

    # ── Flood zone ──────────────────────────────────────────────────────────────
    flood_zone = flood.get("zone") or "N/A"
    flood_desc = flood.get("description") or "Flood zone data not available"
    if flood_zone == "X":
        flood_badge_color = "#27ae60"
    elif flood_zone in ("AE", "A", "VE"):
        flood_badge_color = "#c0392b"
    elif flood_zone and flood_zone != "N/A":
        flood_badge_color = "#e67e22"
    else:
        flood_badge_color = "#95a5a6"

    # ── Flags HTML ──────────────────────────────────────────────────────────────
    flags = report_data.get("flags", [])
    flag_color_map = {
        "red":    ("#fdf2f2", "#c0392b", "&#9888;"),
        "yellow": ("#fffbf0", "#e67e22", "&#9888;"),
        "green":  ("#f0fdf4", "#27ae60", "&#10003;"),
    }
    flag_rows = []
    for flag in flags:
        fc = flag.get("type", "green")
        bg, color, icon = flag_color_map.get(fc, ("#f7f9fb", "#6b7c93", "&#8226;"))
        flag_rows.append(
            f"<tr><td style='padding:8px 0;'>"
            f"<table width='100%' cellpadding='0' cellspacing='0' border='0'><tr>"
            f"<td style='background:{bg};border-left:3px solid {color};border-radius:0 4px 4px 0;padding:9px 14px;'>"
            f"<p style='color:{color};font-size:12px;font-family:Arial,Helvetica,sans-serif;'>{flag.get('text','')}</p>"
            f"</td></tr></table></td></tr>"
        )
    flags_html = "\n".join(flag_rows) if flag_rows else (
        "<tr><td><p style='color:#9aafc4;font-size:12px;font-family:Arial,Helvetica,sans-serif;'>No significant flags detected.</p></td></tr>"
    )

    # ── Permits HTML ──────────────────────────────────────────────────────────
    permits_data = report_data.get("permits", {}) or {}
    permit_list  = permits_data.get("permits", []) if permits_data.get("available") else []
    if permit_list:
        permit_rows = []
        for p in permit_list[:8]:  # cap at 8 for email length
            issued = p.get("issued_date", "—")
            permit_rows.append(
                f"<tr>"
                f"<td style='padding:8px 12px;border-bottom:1px solid #f0f4f8;'><p style='color:#1a2e44;font-size:12px;font-family:Arial,Helvetica,sans-serif;'><strong>{p.get('type','—')}</strong> — {p.get('permit_number','')}</p>"
                f"<p style='color:#9aafc4;font-size:11px;margin-top:2px;font-family:Arial,Helvetica,sans-serif;'>{p.get('description','')[:100]}</p></td>"
                f"<td style='padding:8px 12px;border-bottom:1px solid #f0f4f8;' align='center'><p style='color:#6b7c93;font-size:11px;font-family:Arial,Helvetica,sans-serif;'>{p.get('status','—')}</p></td>"
                f"<td style='padding:8px 12px;border-bottom:1px solid #f0f4f8;' align='right'><p style='color:#6b7c93;font-size:11px;font-family:Arial,Helvetica,sans-serif;'>{issued}</p></td>"
                f"</tr>"
            )
        perm_total = permits_data.get("summary", {}).get("total", len(permit_list))
        permits_html = (
            f"<tr style='background:#f0f5fa;'>"
            f"<td style='padding:8px 12px;'><p style='color:#6b7c93;font-size:10px;font-weight:bold;text-transform:uppercase;letter-spacing:1px;font-family:Arial,Helvetica,sans-serif;'>Permit / Type</p></td>"
            f"<td style='padding:8px 12px;' align='center'><p style='color:#6b7c93;font-size:10px;font-weight:bold;text-transform:uppercase;letter-spacing:1px;font-family:Arial,Helvetica,sans-serif;'>Status</p></td>"
            f"<td style='padding:8px 12px;' align='right'><p style='color:#6b7c93;font-size:10px;font-weight:bold;text-transform:uppercase;letter-spacing:1px;font-family:Arial,Helvetica,sans-serif;'>Issued</p></td>"
            f"</tr>"
            + "\n".join(permit_rows)
            + f"<tr><td colspan='3' style='padding:8px 12px;'><p style='color:#9aafc4;font-size:10px;font-family:Arial,Helvetica,sans-serif;'>{perm_total} total permits on record — Source: {permits_data.get('city','City')} Open Data</p></td></tr>"
        )
    elif permits_data.get("available") is False:
        permits_html = "<tr><td><p style='color:#9aafc4;font-size:12px;font-family:Arial,Helvetica,sans-serif;'>Permit data not available for this city. Check the city permit portal directly.</p></td></tr>"
    else:
        permits_html = "<tr><td><p style='color:#27ae60;font-size:12px;font-family:Arial,Helvetica,sans-serif;'>No permits on record — verify at city portal if recent renovations are present.</p></td></tr>"

    # ── Comps rows HTML ───────────────────────────────────────────────────────
    comps_list = sold_comps.get("comps", []) if isinstance(sold_comps, dict) else []
    comp_rows_list = []
    for i, comp in enumerate(comps_list[:5]):
        row_bg = "#f7f9fb" if i % 2 == 0 else "#ffffff"
        sf   = comp.get("building_sf") or comp.get("sqft") or 0
        price = comp.get("sale_price") or comp.get("price") or 0
        ppf  = f"${price/sf:.0f}" if sf and price else "—"
        comp_rows_list.append(
            f"<tr style='background:{row_bg};'>"
            f"<td style='padding:9px 12px;border-bottom:1px solid #e8edf2;'><p style='color:#1a2e44;font-size:12px;font-family:Arial,Helvetica,sans-serif;'>{comp.get('address','—')}</p></td>"
            f"<td style='padding:9px 12px;border-bottom:1px solid #e8edf2;' align='center'><p style='color:#6b7c93;font-size:12px;font-family:Arial,Helvetica,sans-serif;'>{comp.get('bedrooms','—')}/{comp.get('bathrooms','—')}</p></td>"
            f"<td style='padding:9px 12px;border-bottom:1px solid #e8edf2;' align='center'><p style='color:#6b7c93;font-size:12px;font-family:Arial,Helvetica,sans-serif;'>{'{:,}'.format(int(sf)) if sf else '—'}</p></td>"
            f"<td style='padding:9px 12px;border-bottom:1px solid #e8edf2;' align='right'><p style='color:#1a2e44;font-size:12px;font-weight:bold;font-family:Arial,Helvetica,sans-serif;'>${price:,.0f}</p></td>"
            f"<td style='padding:9px 12px;border-bottom:1px solid #e8edf2;' align='right'><p style='color:#6b7c93;font-size:12px;font-family:Arial,Helvetica,sans-serif;'>{ppf}</p></td>"
            f"<td style='padding:9px 12px;border-bottom:1px solid #e8edf2;' align='center'><p style='color:#6b7c93;font-size:12px;font-family:Arial,Helvetica,sans-serif;'>{comp.get('sale_date','—')}</p></td>"
            f"</tr>"
        )
    comps_rows_html = "\n".join(comp_rows_list) if comp_rows_list else (
        "<tr><td colspan='6' style='padding:14px 12px;'><p style='color:#9aafc4;font-size:12px;font-family:Arial,Helvetica,sans-serif;'>Comparable sales data requires Realie or ATTOM API key.</p></td></tr>"
    )

    # ── Suggested list prices ─────────────────────────────────────────────────
    mkt_mid = market_est.get("market_mid") or (
        ((market_est.get("market_low") or 0) + (market_est.get("market_high") or 0)) / 2
    ) or 0
    list_price_conservative = f"${mkt_mid * 0.95:,.0f}" if mkt_mid else "N/A"
    list_price_market       = f"${mkt_mid:,.0f}" if mkt_mid else "N/A"
    list_price_aggressive   = f"${mkt_mid * 1.05:,.0f}" if mkt_mid else "N/A"
    comp_count = str(len(comps_list))
    comp_radius = "1.0"

    # ── Mortgage / equity ─────────────────────────────────────────────────────
    mort_balance = mortgage.get("balance") or mortgage.get("loan_amount") or 0
    mortgage_balance = f"${mort_balance:,.0f}" if mort_balance else "N/A"
    mortgage_lender  = mortgage.get("lender") or mortgage.get("servicer") or "N/A"
    mortgage_orig_date = mortgage.get("origination_date") or mortgage.get("recording_date") or "N/A"
    mortgage_source  = mortgage.get("source") or "Public records"
    mkt_val = market_est.get("market_mid") or 0
    equity_raw = max(0, mkt_val - mort_balance) if (mkt_val and mort_balance) else None
    equity_estimate = f"${equity_raw:,.0f}" if equity_raw is not None else "N/A"
    current_ltv_pct = f"{(mort_balance/mkt_val*100):.0f}%" if (mkt_val and mort_balance) else "N/A"

    # ── Deal scenarios (use deal_analysis if available, else estimate) ─────────
    ask_price = deal_analysis.get("asking_price") or mkt_val or 0
    def _scenario(price_mult: float) -> dict:
        price = round(ask_price * price_mult / 1000) * 1000
        down_pct = 0.25
        down = round(price * down_pct)
        loan = price - down
        rate_mo = 0.07 / 12
        n = 25 * 12
        try:
            pmt = loan * (rate_mo * (1 + rate_mo)**n) / ((1 + rate_mo)**n - 1)
        except Exception:
            pmt = 0
        # Rough NOI: assessed-based estimate or deal_analysis NOI
        noi = deal_analysis.get("stated_noi") or (assessed_raw * 0.06)  # ~6% cap on assessed
        monthly_cf = (noi / 12) - pmt if pmt else None
        cap = f"{(noi / price * 100):.2f}%" if price else "N/A"
        dscr = f"{(noi / (pmt * 12)):.2f}x" if pmt else "N/A"
        cf_color = "#27ae60" if (monthly_cf and monthly_cf >= 0) else "#c0392b"
        cf_fmt = f"${monthly_cf:+,.0f}/mo" if monthly_cf is not None else "N/A"
        return {
            "price": f"${price:,.0f}",
            "down": f"${down:,.0f}",
            "cashflow": cf_fmt,
            "cf_color": cf_color,
            "cap": cap,
            "dscr": dscr,
        }

    s_conservative = _scenario(0.93) if ask_price else {k: "N/A" for k in ("price","down","cashflow","cf_color","cap","dscr")}
    s_market       = _scenario(1.00) if ask_price else {k: "N/A" for k in ("price","down","cashflow","cf_color","cap","dscr")}
    s_aggressive   = _scenario(1.07) if ask_price else {k: "N/A" for k in ("price","down","cashflow","cf_color","cap","dscr")}

    # ── Assemble final dict ───────────────────────────────────────────────────
    return {
        # Agent
        "agent_name":          agent_name,
        "agent_brokerage":     agent_brokerage,
        "agent_phone":         agent_phone,
        "agent_email":         agent_email,
        "agent_logo_url":      agent_logo_url,
        "agent_initials":      agent_initials,
        # Report metadata
        "report_date":         datetime.utcnow().strftime("%B %d, %Y"),
        # Property
        "property_address":    address,
        "bedrooms":            beds,
        "bathrooms":           baths,
        "building_sf":         building_sf,
        "year_built":          year_built,
        "lot_size":            lot_size,
        "property_type":       property_type,
        "street_view_url":     street_view_url,
        # Valuation
        "assessed_value":      assessed_value,
        "avm_value":           avm_value,
        "avm_methodology":     avm_methodology,
        "avm_source":          avm_source,
        "market_low":          market_low,
        "market_high":         market_high,
        "comp_range":          comp_range,
        "range_low_pct":       "10",
        "range_span_pct":      "80",
        # Ownership
        "owner_name":          owner_name,
        "owner_mailing_address": owner_mailing,
        "hold_duration":       hold_duration,
        "last_sale_price":     last_sale_price,
        # Motivation
        "motivation_score":        mot_score,
        "motivation_tier":         mot_tier,
        "motivation_interpretation": mot_interp,
        "motivation_bg_color":     motivation_bg_color,
        "motivation_text_color":   motivation_text_color,
        "motivation_indicators_html": motivation_indicators_html,
        # Flood
        "flood_zone":          flood_zone,
        "flood_description":   flood_desc,
        "flood_badge_color":   flood_badge_color,
        # Flags
        "flags_html":          flags_html,
        # Permits
        "permits_html":        permits_html,
        # Comps
        "comps_rows_html":     comps_rows_html,
        "comp_count":          comp_count,
        "comp_radius":         comp_radius,
        # List prices
        "list_price_conservative": list_price_conservative,
        "list_price_market":       list_price_market,
        "list_price_aggressive":   list_price_aggressive,
        # Mortgage/equity
        "equity_estimate":     equity_estimate,
        "mortgage_balance":    mortgage_balance,
        "mortgage_lender":     mortgage_lender,
        "mortgage_orig_date":  mortgage_orig_date,
        "mortgage_source":     mortgage_source,
        "current_ltv":         current_ltv_pct,
        # Deal scenarios
        "scenario_conservative_price":    s_conservative["price"],
        "scenario_conservative_down":     s_conservative["down"],
        "scenario_conservative_cashflow": s_conservative["cashflow"],
        "scenario_conservative_cf_color": s_conservative["cf_color"],
        "scenario_conservative_cap":      s_conservative["cap"],
        "scenario_conservative_dscr":     s_conservative["dscr"],
        "scenario_market_price":          s_market["price"],
        "scenario_market_down":           s_market["down"],
        "scenario_market_cashflow":       s_market["cashflow"],
        "scenario_market_cf_color":       s_market["cf_color"],
        "scenario_market_cap":            s_market["cap"],
        "scenario_market_dscr":           s_market["dscr"],
        "scenario_aggressive_price":      s_aggressive["price"],
        "scenario_aggressive_down":       s_aggressive["down"],
        "scenario_aggressive_cashflow":   s_aggressive["cashflow"],
        "scenario_aggressive_cf_color":   s_aggressive["cf_color"],
        "scenario_aggressive_cap":        s_aggressive["cap"],
        "scenario_aggressive_dscr":       s_aggressive["dscr"],
        # Loan assumptions for display
        "loan_ltv_pct":    "75",
        "loan_rate_pct":   "7.0",
        "loan_am_years":   "25",
        "loan_down_pct":   "25",
    }


@app.route("/api/templates/<template_name>", methods=["POST"])
def render_template_endpoint(template_name):
    """
    Render an agent-branded report template.

    POST {
      "address": "...",
      "tier": "pro",
      "agent": {
        "name": "...",
        "brokerage": "...",
        "phone": "...",
        "email": "...",
        "logo_url": "..."
      }
    }

    Returns HTML string ready to email to client.
    Auth: X-Admin-Key header or valid report token.
    """
    # Auth: require admin key OR a valid pipeline tier token
    if not _check_admin():
        # Also allow requests that supply a valid report token
        token = request.headers.get("X-Report-Token") or request.args.get("token", "")
        order = get_order_by_token(token) if (token and len(token) >= 32) else None
        if not order:
            return jsonify({"error": "Unauthorized — provide X-Admin-Key header or valid report token"}), 401

    if template_name not in _ALLOWED_TEMPLATES:
        allowed = ", ".join(sorted(_ALLOWED_TEMPLATES))
        return jsonify({"error": f"Unknown template '{template_name}'. Available: {allowed}"}), 404

    body    = request.get_json(force=True, silent=True) or {}
    address = (body.get("address") or "").strip()
    tier    = body.get("tier", "pro")
    agent   = body.get("agent") or {}

    if not address:
        return jsonify({"error": "address required"}), 400

    if not isinstance(agent, dict):
        agent = {}

    try:
        # Run pipeline to get fresh data (use cache if available)
        cached = report_cache.get(address, tier)
        if cached:
            report_data = cached
        else:
            report_data = run_pipeline(address, tier)
            report_cache.set(address, tier, report_data)

        template_data = _build_template_data(report_data, agent)
        html = _render_agent_template(template_name, template_data)

        return Response(html, mimetype="text/html")

    except FileNotFoundError as e:
        return jsonify({"error": str(e)}), 404
    except Exception as e:
        log.error("Template render failed for %s / %s: %s", template_name, address, e, exc_info=True)
        return jsonify({"error": f"Template render failed: {str(e)}"}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5050))
    debug = os.environ.get("DEBUG", "true").lower() == "true"
    print(f"PropIntel API v1.1 starting on port {port}")
    app.run(host="0.0.0.0", port=port, debug=debug)
