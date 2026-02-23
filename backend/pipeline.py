"""
DealLens Pipeline — Orchestrates all data fetches for a given input.
Input: address or listing URL
Output: structured report data dict
"""
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from scrapers.geocode import geocode
from scrapers.fema import get_flood_zone
from scrapers.census import get_demographics
from scrapers.dcad import search_by_address, search_by_apn
from scrapers.txsos import search_by_address as txsos_address
from scrapers.listing import parse_listing, is_address, detect_source
from motivation import score as motivation_score


def run(input_str: str, tier: str = "starter") -> dict:
    """
    Main pipeline. Run all data fetches and return assembled report data.

    Args:
        input_str: listing URL or property address
        tier: "starter" | "pro"

    Returns:
        dict with all collected data, flags, and motivation score (pro only)
    """
    report = {
        "input": input_str,
        "tier": tier,
        "status": "ok",
        "errors": [],
    }

    # ── Step 1: Determine input type ──────────────────────────────────────────
    if is_address(input_str) or not input_str.startswith("http"):
        address = input_str
        report["input_type"] = "address"
        report["listing"] = {}
    else:
        report["input_type"] = "url"
        report["listing_source"] = detect_source(input_str)

        # Fetch listing data
        listing_data = parse_listing(input_str)
        report["listing"] = listing_data

        # Extract address from listing if possible
        address = (
            listing_data.get("address")
            or listing_data.get("matched_address")
            or listing_data.get("address_raw", {}).get("streetAddress", "")
        )
        if not address:
            # Try to extract from URL for LoopNet
            addr_m = re.search(r"/(\d+[-\w]+(?:-[A-Z]{2})/)", input_str, re.IGNORECASE)
            if addr_m:
                address = addr_m.group(1).replace("-", " ")

    report["resolved_address"] = address

    # ── Step 2: Geocode ───────────────────────────────────────────────────────
    geo = geocode(address) if address else {"error": "No address resolved"}
    report["geo"] = geo

    if geo.get("error"):
        report["errors"].append(f"Geocode: {geo['error']}")

    lat = geo.get("lat")
    lng = geo.get("lng")
    zip_code = geo.get("zip", "")

    # ── Step 3: Parallel data fetches ─────────────────────────────────────────
    tasks = {}
    with ThreadPoolExecutor(max_workers=4) as ex:
        if lat and lng:
            tasks["fema"] = ex.submit(get_flood_zone, lat, lng)
        if zip_code:
            tasks["census"] = ex.submit(get_demographics, zip_code)
        if address:
            tasks["dcad"] = ex.submit(search_by_address, address)
        if address and tier == "pro":
            # Parse just the street number + name for TX SOS search
            street_match = re.match(r"(\d+\s+[\w\s]+?)(?:,|\s+\w{2}\s+\d{5})", address)
            street = street_match.group(1).strip() if street_match else address
            city = geo.get("city", "")
            tasks["txsos"] = ex.submit(txsos_address, street, city)

        results = {}
        for key, future in tasks.items():
            try:
                results[key] = future.result(timeout=20)
            except Exception as e:
                results[key] = {"error": str(e)}
                report["errors"].append(f"{key}: {str(e)}")

    report["parcel"] = results.get("dcad", {})
    report["flood"] = results.get("fema", {})
    report["demographics"] = results.get("census", {})
    report["businesses"] = results.get("txsos", [])

    # ── Step 4: Pro-only enrichment ───────────────────────────────────────────
    if tier == "pro":
        owner_name = report["parcel"].get("owner_name", "")

        # Motivation scoring — from verified data only
        listing_meta = {
            "days_on_market": report["listing"].get("days_on_market"),
            "price_reduced": report["listing"].get("price_reduced", False),
            "price_reduction_amount": report["listing"].get("price_reduction_amount", 0),
        }
        report["motivation"] = motivation_score(
            parcel=report["parcel"],
            listing=listing_meta,
            deed_history=report["parcel"].get("deed_history", []),
        )

        # Skip trace placeholder (DataZapp API — requires key)
        report["skip_trace"] = {
            "status": "pending_api_key",
            "note": "DataZapp skip trace — add DATAZAPP_API_KEY to .env to enable",
            "owner_name": owner_name,
        }

        # Lien search placeholder
        report["liens"] = {
            "status": "pending",
            "note": "Dallas County Clerk lien search — implement county clerk scraper",
            "manual_url": "https://www.dallascounty.org/departments/countyclerk/real-property.php"
        }

    # ── Step 5: Assemble flags ────────────────────────────────────────────────
    report["flags"] = _build_flags(report)

    return report


def _build_flags(report: dict) -> list:
    """Generate human-readable flags from collected data."""
    flags = []
    flood = report.get("flood", {})
    parcel = report.get("parcel", {})
    motivation = report.get("motivation", {})

    # Flood zone
    zone = flood.get("zone", "")
    if zone == "X":
        flags.append({"type": "green", "text": f"Flood Zone {zone} — Minimal risk, no flood insurance required (FEMA)"})
    elif zone in ("AE", "A", "VE"):
        flags.append({"type": "red", "text": f"⚠ Flood Zone {zone} — High flood risk, flood insurance REQUIRED (FEMA)"})
    elif zone:
        flags.append({"type": "yellow", "text": f"Flood Zone {zone} — Verify at msc.fema.gov (FEMA)"})

    # Tax delinquency
    if parcel.get("tax_delinquent"):
        flags.append({"type": "red", "text": "🚩 Tax delinquency on record — verify amount and payoff (DCAD)"})
    elif parcel.get("owner_name"):
        flags.append({"type": "green", "text": "No tax delinquency detected — taxes current (DCAD)"})

    # Motivation
    if motivation:
        tier = motivation.get("tier", "")
        score = motivation.get("score", 0)
        if tier == "HIGH":
            flags.append({"type": "yellow", "text": f"Motivation score {score}/100 (HIGH) — seller signals present, see full breakdown"})
        elif tier == "MODERATE":
            flags.append({"type": "yellow", "text": f"Motivation score {score}/100 (MODERATE) — some signals detected"})

    # Businesses
    businesses = report.get("businesses", [])
    forfeited = [b for b in businesses if b.get("status_flag") == "red"]
    if forfeited:
        flags.append({"type": "red", "text": f"⚠ {len(forfeited)} forfeited/dissolved entity found at address — possible vacancy (TX SOS)"})

    return flags
