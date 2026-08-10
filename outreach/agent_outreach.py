"""
PropIntel Agent Outreach Engine
================================
Workflow:
  1. Pull active listings for a zip code (Zillow via RapidAPI or scrape)
  2. Extract property address + listing agent name/email
  3. Run PropIntel pipeline on each address → generate report token
  4. Send personalized cold email to agent: "We ran a report on your listing"

Usage:
  python agent_outreach.py --zip 75204 --limit 20 --dry-run
  python agent_outreach.py --zip 75204 --limit 20 --send

Requires:
  ZILLOW_RAPIDAPI_KEY  → RapidAPI Zillow API key (free tier: 100/mo)
  MAILGUN_API_KEY      → already configured
  PROPINTEL_ADMIN_KEY  → to generate report tokens
  PROPINTEL_API_URL    → https://propintel-htij.onrender.com

Cost per lead: ~$0.00 (free tier) + $0.01/email (Mailgun)
"""

import os
import sys
import json
import time
import argparse
import csv
import urllib.request
import urllib.parse
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

RAPIDAPI_KEY     = os.environ.get("ZILLOW_RAPIDAPI_KEY", "")
MAILGUN_API_KEY  = os.environ.get("MAILGUN_API_KEY", "")
MAILGUN_DOMAIN   = os.environ.get("MAILGUN_DOMAIN", "mathislandco.com")
ADMIN_KEY        = os.environ.get("PROPINTEL_ADMIN_KEY", "propintel-mason-2026")
API_URL          = os.environ.get("PROPINTEL_API_URL", "https://propintel-htij.onrender.com")
HUNTER_API_KEY   = os.environ.get("HUNTER_API_KEY", "")
FROM_EMAIL       = "mason@mathislandco.com"
FROM_NAME        = "Mason — PropIntel"


# ── ZILLOW LISTING SEARCH ─────────────────────────────────────────────────────

def search_listings_zillow(zip_code: str, limit: int = 20) -> list:
    """
    Pull active For Sale listings from Zillow via RapidAPI.
    Returns list of { address, price, beds, baths, sqft, agent_name, agent_email, listing_url }
    
    RapidAPI Zillow: https://rapidapi.com/apimaker/api/zillow-com1
    Free tier: 100 calls/month
    """
    if not RAPIDAPI_KEY:
        log.warning("ZILLOW_RAPIDAPI_KEY not set — using mock data for testing")
        return _mock_listings(zip_code)

    url = "https://zillow-com1.p.rapidapi.com/propertySearch"
    params = {
        "location": zip_code,
        "listingType": "For Sale",
        "page": "1",
    }
    req = urllib.request.Request(
        f"{url}?{urllib.parse.urlencode(params)}",
        headers={
            "X-RapidAPI-Key": RAPIDAPI_KEY,
            "X-RapidAPI-Host": "zillow-com1.p.rapidapi.com",
        }
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())
            return _parse_zillow_results(data, limit)
    except Exception as e:
        log.error("Zillow API error: %s", e)
        return []


def _parse_zillow_results(data: dict, limit: int) -> list:
    """Parse Zillow API response into clean listing objects."""
    listings = []
    props = data.get("props") or data.get("results") or []
    for p in props[:limit]:
        agent_name  = p.get("listedBy") or p.get("agentName") or ""
        brokerage   = p.get("brokerName") or p.get("brokerage") or ""
        listings.append({
            "address":        p.get("address") or p.get("streetAddress", ""),
            "city":           p.get("city", ""),
            "state":          p.get("state", "TX"),
            "zip":            p.get("zipcode") or p.get("zip", ""),
            "price":          p.get("price") or p.get("listPrice"),
            "price_fmt":      f"${p.get('price',0):,.0f}" if p.get("price") else "",
            "beds":           p.get("bedrooms") or p.get("beds"),
            "baths":          p.get("bathrooms") or p.get("baths"),
            "sqft":           p.get("livingArea") or p.get("sqft"),
            "agent_name":     agent_name,
            "brokerage_name": brokerage,
            "agent_email":    "",   # Zillow doesn't expose email — enriched separately
            "listing_url":    p.get("detailUrl") or p.get("url") or "",
            "zpid":           p.get("zpid") or "",
        })
    return listings


def _mock_listings(zip_code: str) -> list:
    """Mock listings for testing without an API key."""
    return [
        {
            "address": "3625 McKinney Ave", "city": "Dallas", "state": "TX",
            "zip": zip_code, "price": 1_450_000, "price_fmt": "$1,450,000",
            "beds": None, "baths": None, "sqft": 7200,
            "agent_name": "Sarah Johnson", "brokerage_name": "Keller Williams Realty",
            "agent_email": "",
            "listing_url": "https://zillow.com/homedetails/...", "zpid": "mock1",
        },
        {
            "address": "4200 Live Oak St", "city": "Dallas", "state": "TX",
            "zip": zip_code, "price": 875_000, "price_fmt": "$875,000",
            "beds": 3, "baths": 2, "sqft": 2100,
            "agent_name": "Mike Torres", "brokerage_name": "Compass",
            "agent_email": "",
            "listing_url": "https://zillow.com/homedetails/...", "zpid": "mock2",
        },
    ]


# ── AGENT EMAIL ENRICHMENT ────────────────────────────────────────────────────

# Known brokerage slug → domain mappings (expand as needed)
_BROKERAGE_DOMAIN_MAP = {
    "kellerwilliams":  "kw.com",
    "kw":              "kw.com",
    "compass":         "compass.com",
    "century21":       "century21.com",
    "coldwellbanker":  "coldwellbanker.com",
    "remax":           "remax.com",
    "redfin":          "redfin.com",
    "berkshirehathaway": "bhhsrealestate.com",
    "bhhs":            "bhhsrealestate.com",
    "sothebys":        "sothebysrealty.com",
    "sothebysrealty":  "sothebysrealty.com",
    "erareal":         "era.com",
    "era":             "era.com",
    "exitrealty":      "exitrealty.com",
    "exp":             "exprealty.com",
    "exprealty":       "exprealty.com",
    "expagent":        "exprealty.com",
    "weichert":        "weichert.com",
    "betterhomesgardens": "bhgre.com",
    "bhgre":           "bhgre.com",
    "longfoster":      "longandfoster.com",
    "longandfoster":   "longandfoster.com",
    "corcoran":        "corcorangroup.com",
    "douglas":         "elliman.com",
    "douglaselliman":  "elliman.com",
    "elliman":         "elliman.com",
    "alain":           "apuy.com",
    "jllresidential":  "us.jll.com",
}

# Words to strip when normalising a brokerage name
_STRIP_WORDS = [
    "real estate", "realty", "properties", "property",
    "group", "team", "associates", "agency", "brokerage",
    "llc", "inc", "corp", "co", "ltd",
]


def get_brokerage_domain(brokerage_name: str) -> str:
    """
    Convert a brokerage display name to an email domain heuristic.

    Examples:
        "Keller Williams Realty"  → "kw.com"
        "Century 21 Group"        → "century21.com"
        "Acme Homes LLC"          → "acmehomes.com"

    Returns a domain string (no protocol) or "" if name is blank.
    """
    if not brokerage_name or not brokerage_name.strip():
        return ""

    cleaned = brokerage_name.lower()

    # Strip common noise words
    for word in _STRIP_WORDS:
        cleaned = cleaned.replace(word, " ")

    # Remove non-alphanumeric (except spaces) and collapse whitespace
    import re as _re
    cleaned = _re.sub(r"[^a-z0-9 ]", "", cleaned)
    cleaned = _re.sub(r"\s+", "", cleaned).strip()

    if not cleaned:
        return ""

    # Check known map first
    if cleaned in _BROKERAGE_DOMAIN_MAP:
        return _BROKERAGE_DOMAIN_MAP[cleaned]

    # Partial-match check (e.g. "kellerwilliamsdfw" → "kw.com")
    for key, domain in _BROKERAGE_DOMAIN_MAP.items():
        if key in cleaned or cleaned in key:
            return domain

    # Fallback: best-guess .com
    return f"{cleaned}.com"


def hunter_find_email(first_name: str, last_name: str, domain: str) -> str:
    """
    Look up an agent's business email via the Hunter.io Email Finder API.

    Args:
        first_name: Agent's first name.
        last_name:  Agent's last name.
        domain:     Brokerage email domain (e.g. "kw.com").

    Returns:
        Email string if Hunter returns confidence >= 70, else "".
        Returns "" (with a logged warning) if HUNTER_API_KEY is not set.
    """
    if not HUNTER_API_KEY:
        log.warning("HUNTER_API_KEY not set — skipping Hunter.io email lookup")
        return ""

    if not domain or not first_name or not last_name:
        return ""

    params = urllib.parse.urlencode({
        "domain":     domain,
        "first_name": first_name,
        "last_name":  last_name,
        "api_key":    HUNTER_API_KEY,
    })
    url = f"https://api.hunter.io/v2/email-finder?{params}"

    try:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))

        email      = (data.get("data") or {}).get("email") or ""
        confidence = (data.get("data") or {}).get("score") or 0

        if email and confidence >= 70:
            log.info("Hunter found email %s (confidence %d) for %s %s @ %s",
                     email, confidence, first_name, last_name, domain)
            return email

        log.info("Hunter low-confidence (%d) for %s %s @ %s — skipping",
                 confidence, first_name, last_name, domain)
        return ""

    except Exception as exc:
        log.warning("Hunter.io API error: %s", exc)
        return ""


def enrich_agent_email(agent_name: str, brokerage: str = "",
                       city: str = "Dallas", state: str = "TX") -> str:
    """
    Attempt to find an agent's email.

    Strategy (in order):
      1. Hunter.io Email Finder (requires HUNTER_API_KEY + brokerage name)
      2. Return "" — let the caller fall back to manual enrichment

    Args:
        agent_name: Full name of the listing agent (e.g. "Sarah Johnson").
        brokerage:  Brokerage display name (e.g. "Keller Williams Realty").
        city/state: Location context (reserved for future lookup strategies).

    Returns:
        Email string or "".
    """
    if not agent_name or not agent_name.strip():
        return ""

    parts = agent_name.strip().split()
    first = parts[0] if parts else ""
    last  = parts[-1] if len(parts) > 1 else ""

    # ── Strategy 1: Hunter.io (best hit rate for brokerage emails) ──
    if brokerage and first and last:
        domain = get_brokerage_domain(brokerage)
        if domain:
            email = hunter_find_email(first, last, domain)
            if email:
                return email

    # ── No email found ──
    return ""


# ── PROPINTEL REPORT GENERATION ──────────────────────────────────────────────

def generate_report_token(address: str, city: str, state: str, zip_code: str) -> str:
    """
    Call PropIntel API to generate a report and get back a public token URL.
    Returns the full report URL or "" on failure.
    """
    full_address = f"{address}, {city}, {state} {zip_code}".strip(", ")

    payload = json.dumps({
        "address": full_address,
        "email":   "",
        "tier":    "pro",
        "key":     ADMIN_KEY,
    }).encode("utf-8")

    req = urllib.request.Request(
        f"{API_URL}/api/admin/test-email",
        data=payload,
        headers={
            "Content-Type": "application/json",
            "X-Admin-Key": ADMIN_KEY,
        },
        method="POST"
    )
    # We don't use test-email here — use /api/analyze to get a token
    # TODO: add a GET /api/admin/generate-token endpoint that returns token only
    # For now return the demo URL
    encoded_addr = urllib.parse.quote(full_address)
    return f"https://masecfc03-ui.github.io/propintel/report.html?address={encoded_addr}&tier=pro&demo=1"


# ── EMAIL TEMPLATES ───────────────────────────────────────────────────────────

def build_email(agent_name: str, address: str, price_fmt: str,
                report_url: str) -> tuple:
    """Returns (subject, html_body) for the outreach email."""
    first = agent_name.split()[0].title() if agent_name else "there"
    short_addr = address.split(",")[0].strip()

    subject = f"I ran a property intel report on your listing at {short_addr}"

    html = f"""
<div style="font-family:Arial,sans-serif;max-width:600px;color:#0f172a;font-size:15px;line-height:1.6">
  <p>Hey {first},</p>

  <p>I noticed you have <strong>{short_addr}</strong> listed{(' at ' + price_fmt) if price_fmt else ''}.
  I ran it through <strong>PropIntel</strong> — our property intelligence tool — and pulled together
  a full analysis: assessed value vs. market range, FEMA flood zone, owner intel, financial estimates,
  neighborhood demographics, and comparable sales.</p>

  <p>Here's the report:</p>

  <p style="margin:24px 0">
    <a href="{report_url}"
       style="background:#2563eb;color:white;padding:12px 24px;border-radius:6px;
              text-decoration:none;font-weight:600;display:inline-block">
      View Property Report →
    </a>
  </p>

  <p>PropIntel pulls from county appraisal records, FEMA, Census, and Texas Secretary of State —
  all in one clean report. Agents use it to add depth to client conversations and listing
  presentations. One-time pulls, no subscription.</p>

  <p>Let me know what you think — happy to run one on any property you're working.</p>

  <p>Mason<br>
  <a href="https://propertyvalueintel.com" style="color:#2563eb">propertyvalueintel.com</a></p>

  <hr style="border:none;border-top:1px solid #e2e8f0;margin:24px 0">
  <p style="font-size:11px;color:#94a3b8">
    You're receiving this because you're a licensed agent in the DFW market.
    To stop receiving these emails, reply "unsubscribe" and I'll remove you immediately.
  </p>
</div>
"""
    return subject, html


# ── SEND EMAIL ────────────────────────────────────────────────────────────────

def send_email(to_email: str, to_name: str, subject: str, html: str) -> bool:
    """Send via Mailgun. Returns True on success."""
    import base64, subprocess, tempfile, os as _os

    url = f"https://api.mailgun.net/v3/{MAILGUN_DOMAIN}/messages"
    credentials = base64.b64encode(f"api:{MAILGUN_API_KEY}".encode()).decode()

    # Write HTML to temp file for curl
    with tempfile.NamedTemporaryFile(mode='w', suffix='.html', delete=False) as f:
        f.write(html)
        tmp_path = f.name

    try:
        result = subprocess.run([
            "curl", "-s", "--user", f"api:{MAILGUN_API_KEY}",
            url,
            "-F", f"from={FROM_NAME} <{FROM_EMAIL}>",
            "-F", f"to={to_name} <{to_email}>",
            "-F", f"subject={subject}",
            "-F", f"html=<{tmp_path}",
        ], capture_output=True, text=True, timeout=15)

        resp = json.loads(result.stdout) if result.stdout else {}
        success = bool(resp.get("id") or "queued" in resp.get("message", "").lower())
        if not success:
            log.warning("Mailgun failed: %s", result.stdout[:200])
        return success
    except Exception as e:
        log.error("Send error: %s", e)
        return False
    finally:
        _os.unlink(tmp_path)


# ── MAIN ORCHESTRATOR ─────────────────────────────────────────────────────────

def run_outreach(zip_codes: list, limit: int = 20, dry_run: bool = True,
                 output_csv: str = "outreach-results.csv"):
    """
    Full pipeline: find listings → enrich → generate report → send (or dry-run).
    """
    all_results = []

    for zip_code in zip_codes:
        log.info("Searching listings in %s...", zip_code)
        listings = search_listings_zillow(zip_code, limit)
        log.info("Found %d listings in %s", len(listings), zip_code)

        for listing in listings:
            address    = listing["address"]
            city       = listing.get("city", "")
            state      = listing.get("state", "TX")
            lzip       = listing.get("zip", zip_code)
            agent_name     = listing.get("agent_name", "")
            brokerage_name = listing.get("brokerage_name", "")
            agent_email    = listing.get("agent_email", "")

            # Try to enrich email if missing (Hunter.io first, then fallback)
            if not agent_email and agent_name:
                agent_email = enrich_agent_email(agent_name, brokerage_name, city, state)

            # Generate report URL
            report_url = generate_report_token(address, city, state, lzip)

            # Build email
            subject, html = build_email(agent_name, address, listing.get("price_fmt", ""), report_url)

            result = {
                "address":        address,
                "city":           city,
                "zip":            lzip,
                "price":          listing.get("price_fmt", ""),
                "agent_name":     agent_name,
                "brokerage_name": brokerage_name,
                "agent_email":    agent_email,
                "report_url":     report_url,
                "email_subject":  subject,
                "sent":           False,
                "sent_at":        "",
                "error":          "",
            }

            if agent_email and not dry_run:
                log.info("Sending to %s <%s> — %s", agent_name, agent_email, address)
                ok = send_email(agent_email, agent_name, subject, html)
                result["sent"]    = ok
                result["sent_at"] = datetime.utcnow().isoformat() if ok else ""
                if not ok:
                    result["error"] = "Mailgun failed"
                time.sleep(1)   # gentle rate limit
            elif not agent_email:
                log.info("No email for %s — %s (needs manual enrichment)", agent_name, address)
                result["error"] = "no email"
            else:
                log.info("[DRY RUN] Would send to %s <%s> — %s", agent_name, agent_email, address)
                result["sent"] = "dry_run"

            all_results.append(result)

    # Export CSV
    if all_results:
        with open(output_csv, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=all_results[0].keys())
            writer.writeheader()
            writer.writerows(all_results)
        log.info("Results saved → %s (%d rows)", output_csv, len(all_results))

    # Summary
    sent    = sum(1 for r in all_results if r["sent"] is True)
    no_email= sum(1 for r in all_results if r["error"] == "no email")
    total   = len(all_results)
    log.info("Done. Total: %d | Sent: %d | No email: %d | Dry run: %s",
             total, sent, no_email, dry_run)

    return all_results


# ── CLI ───────────────────────────────────────────────────────────────────────

BROKERAGE_ANGLES = {
    "compass":          "Compass agents move fast — PropIntel gives you the data layer to back it up.",
    "keller williams":  "KW's systems mindset + PropIntel's data depth = unfair advantage in client meetings.",
    "kw":               "KW's systems mindset + PropIntel's data depth = unfair advantage in client meetings.",
    "remax":            "Running independently means every edge counts. This is one most agents don't have yet.",
    "re/max":           "Running independently means every edge counts. This is one most agents don't have yet.",
    "coldwell banker":  "Your luxury clients expect white-glove prep. Branded reports deliver it.",
    "ebby halliday":    "Ebby agents set the standard in DFW. PropIntel helps you stay ahead of it.",
    "dave perry miller":"Your clients expect white-glove. Branded reports deliver it.",
    "allie beth":       "Your clients expect white-glove. Branded reports deliver it.",
    "berkshire":        "Premium service deserves premium data. PropIntel fills the gaps MLS can't.",
}

def build_agent_email(agent_name, brokerage=""):
    """Build a short, tight cold email for a known agent (no listing needed)."""
    first = agent_name.split()[0].title() if agent_name else "there"
    brokerage_lower = (brokerage or "").lower()

    angle = ""
    for key, val in BROKERAGE_ANGLES.items():
        if key in brokerage_lower:
            angle = val
            break

    subject = "Quick question, {}".format(first)

    html = """
<div style="font-family:Arial,sans-serif;max-width:560px;color:#0f172a;font-size:15px;line-height:1.6">
  <p>Hi {first},</p>

  <p>Quick offer: send me any DFW address you're evaluating — listing, buyer prospect, or comp check —
  and I'll run a full PropIntel property report on it for free.</p>

  <p>What comes back in 60 seconds:</p>
  <ul>
    <li>Owner motivation score + hold duration</li>
    <li>Equity position and financing history</li>
    <li>Sold comps with price-per-sqft</li>
    <li>Flood zone, liens, tax status</li>
  </ul>

  {angle_p}

  <p>Worth trying? Just reply with an address.</p>

  <p>Mason Mathis<br>
  PropIntel —
  <a href="https://propertyvalueintel.com" style="color:#2563eb">propertyvalueintel.com</a></p>

  <hr style="border:none;border-top:1px solid #e2e8f0;margin:20px 0">
  <p style="font-size:11px;color:#94a3b8">
    You're a licensed agent in DFW — that's why you're getting this.
    Reply "unsubscribe" and I'll remove you immediately.
  </p>
</div>
""".format(
        first=first,
        angle_p='<p><em>{}</em></p>'.format(angle) if angle else "",
    )
    return subject, html


def run_outreach_from_csv(csv_path, dry_run=True, output_csv="outreach-csv-results.csv", limit=0):
    """
    Read agents from a CSV file and send cold outreach emails.
    CSV columns expected: agent_name, brokerage, email, phone, website, source, scraped_at
    email_guessed column optional (True/False).
    """
    import csv as _csv

    results = []
    with open(csv_path, newline="") as f:
        reader = _csv.DictReader(f)
        rows = list(reader)

    if limit:
        rows = rows[:limit]

    log.info("Loaded %d agents from %s", len(rows), csv_path)

    for row in rows:
        agent_name  = row.get("agent_name", "").strip()
        brokerage   = row.get("brokerage", "").strip()
        email       = row.get("email", "").strip()
        is_guessed  = row.get("email_guessed", "False").strip().lower() == "true"

        if not email:
            log.info("Skipping %s — no email", agent_name)
            results.append({**row, "sent": False, "error": "no_email", "sent_at": ""})
            continue

        subject, html = build_agent_email(agent_name, brokerage)

        result = {**row, "email_subject": subject, "sent": False, "sent_at": "", "error": ""}

        if not dry_run:
            log.info("Sending to %s <%s>%s", agent_name, email, " [GUESSED EMAIL]" if is_guessed else "")
            ok = send_email(email, agent_name, subject, html)
            result["sent"]    = ok
            result["sent_at"] = datetime.utcnow().isoformat() if ok else ""
            if not ok:
                result["error"] = "mailgun_failed"
            time.sleep(1.5)
        else:
            log.info("[DRY RUN] %s → %s | guessed=%s", agent_name, email, is_guessed)
            log.info("  Subject: %s", subject)
            result["sent"] = "dry_run"

        results.append(result)

    if results:
        with open(output_csv, "w", newline="") as f:
            writer = _csv.DictWriter(f, fieldnames=results[0].keys())
            writer.writeheader()
            writer.writerows(results)
        log.info("Results saved → %s", output_csv)

    total    = len(results)
    sent_ok  = sum(1 for r in results if r["sent"] is True)
    no_email = sum(1 for r in results if r.get("error") == "no_email")
    log.info("Done. Total: %d | Sent: %d | No email: %d | Mode: %s",
             total, sent_ok, no_email, "DRY RUN" if dry_run else "LIVE")
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PropIntel Agent Outreach")
    parser.add_argument("--zip",   nargs="+", default=["75204"], help="Zip codes to target (Zillow mode)")
    parser.add_argument("--csv",   default="",                   help="Path to agents CSV (CSV mode — bypasses Zillow)")
    parser.add_argument("--limit", type=int, default=20,         help="Max agents/listings to process")
    parser.add_argument("--send",  action="store_true",          help="Actually send emails (default: dry run)")
    parser.add_argument("--out",   default="outreach-results.csv", help="Output CSV path")
    args = parser.parse_args()

    if args.csv:
        # CSV mode — read real agents from file, send direct outreach
        run_outreach_from_csv(
            csv_path=args.csv,
            dry_run=not args.send,
            output_csv=args.out,
            limit=args.limit,
        )
    else:
        # Zip mode — search Zillow listings, enrich, send
        run_outreach(
            zip_codes=args.zip,
            limit=args.limit,
            dry_run=not args.send,
            output_csv=args.out,
        )
