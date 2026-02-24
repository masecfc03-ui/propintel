#!/usr/bin/env python3
"""
PropIntel Scraper Health Monitor
Runs test lookups on known properties for each scraper.
Fires Telegram alert if any scraper returns empty/error when it should have data.

Run via: python3 backend/scripts/health_check_scrapers.py
"""

import sys
import os
import json
import urllib.request
import urllib.parse
import ssl

# macOS Python 3.9 SSL cert fix
_ssl_ctx = ssl.create_default_context()
_ssl_ctx.check_hostname = False
_ssl_ctx.verify_mode = ssl.CERT_NONE
_https_handler = urllib.request.HTTPSHandler(context=_ssl_ctx)
_opener = urllib.request.build_opener(_https_handler)
urllib.request.install_opener(_opener)
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

TELEGRAM_TOKEN = "8526813771:AAEM2q-_VgHfHjzRIUJLCBhATaQrVtVxzUI"
TELEGRAM_CHAT = "8245186551"

# Known test properties — each should return data from its scraper
TEST_CASES = [
    {
        "scraper": "dcad",
        "address": "3229 Forest Ln, Garland TX 75042",
        "expected_field": "owner_name",
        "geo": {"county": "dallas", "city": "garland"}
    },
    {
        "scraper": "realie",
        "address": "559 Hawken Dr, Coppell TX 75019",
        "expected_field": "ownerName",
        "geo": {"county": "dallas", "city": "coppell"}
    },
]

def send_telegram(msg):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    data = urllib.parse.urlencode({"chat_id": TELEGRAM_CHAT, "text": msg}).encode()
    try:
        urllib.request.urlopen(urllib.request.Request(url, data=data), timeout=10)
    except Exception as e:
        print(f"Telegram failed: {e}", file=sys.stderr)

def check_dcad(address, geo):
    try:
        from scrapers.dcad import search_by_address
        result = search_by_address(address)
        return result, not result.get("error") and result.get("owner_name")
    except Exception as e:
        return {"error": str(e)}, False

def check_realie(address, geo):
    try:
        from scrapers.realie import get_property_detail
        result = get_property_detail(address)
        return result, not result.get("error") and result.get("ownerName")
    except Exception as e:
        return {"error": str(e)}, False

def check_county_router(address, geo):
    try:
        from scrapers.county_router import get_parcel_data
        result = get_parcel_data(address, geo)
        return result, not result.get("error")
    except Exception as e:
        return {"error": str(e)}, False

def check_permits(address, geo):
    try:
        from scrapers.permits import get_permits
        result = get_permits(address, geo)
        # permits available=False is OK (just no data) — only alert if exception
        return result, True
    except Exception as e:
        return {"error": str(e)}, False

CHECKERS = {
    "dcad": check_dcad,
    "realie": check_realie,
    "county_router": check_county_router,
    "permits": check_permits,
}

def run_checks():
    failures = []
    results = []

    # Run all checks
    checks = [
        ("dcad", "3229 Forest Ln, Garland TX 75042", {"county": "dallas", "city": "garland"}),
        ("realie", "559 Hawken Dr, Coppell TX 75019", {"county": "dallas", "city": "coppell"}),
        ("county_router", "4747 Greenville Ave, Dallas TX 75206", {"county": "dallas", "city": "dallas"}),
        ("permits", "1234 Main St, Dallas TX 75201", {"county": "dallas", "city": "dallas"}),
    ]

    for scraper_name, address, geo in checks:
        checker = CHECKERS.get(scraper_name)
        if not checker:
            continue
        try:
            result, ok = checker(address, geo)
            status = "✅" if ok else "❌"
            results.append(f"{status} {scraper_name}: {address[:30]}")
            if not ok:
                failures.append(f"❌ {scraper_name}: {result.get('error', 'No data returned')}")
        except Exception as e:
            failures.append(f"❌ {scraper_name}: exception — {e}")
            results.append(f"❌ {scraper_name}: exception")

    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

    if failures:
        msg = f"🚨 PropIntel Scraper Alert — {ts}\n\n" + "\n".join(failures) + "\n\nFull results:\n" + "\n".join(results)
        send_telegram(msg)
        print("ALERT sent:", msg)
        return 1
    else:
        # Silent pass — only alert on failures
        print(f"[{ts}] All scrapers healthy: " + " | ".join(results))
        return 0

if __name__ == "__main__":
    sys.exit(run_checks())
