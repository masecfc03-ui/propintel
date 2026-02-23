"""
PropIntel Pipeline — Orchestrates all data fetches for a given input.
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
from scrapers.datazapp import skip_trace, parse_owner_name
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

        import concurrent.futures as _cf
        results = {}
        for key, future in tasks.items():
            try:
                results[key] = future.result(timeout=30)
            except _cf.TimeoutError:
                results[key] = {"error": "timeout", "warning": "Request timed out (30s) — service may be slow from this region"}
                report["errors"].append(f"{key}: timeout")
            except Exception as e:
                err_msg = str(e) or repr(e)
                results[key] = {"error": err_msg}
                report["errors"].append(f"{key}: {err_msg}")

    report["parcel"] = results.get("dcad", {})
    report["flood"] = results.get("fema", {})
    report["demographics"] = results.get("census", {})
    report["businesses"] = results.get("txsos", [])

    # ── Step 4: Pro-only enrichment ───────────────────────────────────────────
    if tier == "pro":
        parcel_data = report["parcel"]
        owner_name = parcel_data.get("owner_name", "")

        # Motivation scoring — from verified data only
        listing_meta = {
            "days_on_market": report["listing"].get("days_on_market"),
            "price_reduced": report["listing"].get("price_reduced", False),
            "price_reduction_amount": report["listing"].get("price_reduction_amount", 0),
        }
        report["motivation"] = motivation_score(
            parcel=parcel_data,
            listing=listing_meta,
            deed_history=parcel_data.get("deed_history", []),
        )

        # DataZapp skip trace
        first, last, is_entity = parse_owner_name(owner_name)
        owner_mail_addr = parcel_data.get("owner_mailing", "")
        owner_city = parcel_data.get("owner_city", "")
        owner_state = parcel_data.get("owner_state", "TX")
        owner_zip = parcel_data.get("owner_zip", "")

        if is_entity:
            # For entities, try to skip trace the entity name
            from scrapers.datazapp import skip_trace_entity
            report["skip_trace"] = skip_trace_entity(
                entity_name=owner_name,
                mailing_address=owner_mail_addr,
                mailing_city=owner_city,
                mailing_state=owner_state,
                mailing_zip=owner_zip,
            )
        elif first or last:
            report["skip_trace"] = skip_trace(
                first_name=first,
                last_name=last,
                address=owner_mail_addr,
                city=owner_city,
                state=owner_state,
                zip_code=owner_zip,
            )
        else:
            report["skip_trace"] = {
                "status": "no_owner",
                "phones": [],
                "emails": [],
                "note": "Owner name not available from DCAD — manual skip trace required.",
                "source": "DataZapp",
            }

        # Lien search placeholder
        report["liens"] = {
            "status": "manual_required",
            "note": "Dallas County Clerk lien search — access via link below.",
            "manual_url": "https://www.dallascounty.org/departments/countyclerk/real-property.php",
            "apn": parcel_data.get("apn", ""),
        }

        # Deal analyzer calculations (from listing data if available)
        report["deal_analysis"] = _analyze_deal(report)

    # ── Step 5: Assemble flags ────────────────────────────────────────────────
    report["flags"] = _build_flags(report)

    return report


def _analyze_deal(report: dict) -> dict:
    """
    Calculate key deal metrics from available listing + parcel data.
    Only uses numbers that come from verified sources (listing, DCAD).
    Marks estimates clearly.
    """
    listing = report.get("listing", {})
    parcel  = report.get("parcel", {})

    ask_price = listing.get("asking_price")
    cap_rate_str = listing.get("cap_rate", "")
    bldg_sf = None
    try:
        bldg_sf = float(str(listing.get("building_sf") or parcel.get("building_sf") or 0).replace(",", ""))
    except Exception:
        pass

    # Parse stated cap rate
    stated_cap = None
    if cap_rate_str:
        try:
            stated_cap = float(str(cap_rate_str).replace("%", "").strip()) / 100
        except Exception:
            pass

    # NOI from stated cap rate + asking price
    stated_noi = None
    if ask_price and stated_cap:
        stated_noi = round(ask_price * stated_cap)

    # Price per SF
    price_per_sf = None
    if ask_price and bldg_sf and bldg_sf > 0:
        price_per_sf = round(ask_price / bldg_sf, 2)

    # Assessed vs asking premium/discount
    assessed_total = parcel.get("assessed_total")
    assessed_premium = None
    if ask_price and assessed_total and assessed_total > 0:
        assessed_premium = round((ask_price / assessed_total - 1) * 100, 1)

    # DSCR at 75% LTV, 7% rate, 25yr am (commercial market standard)
    dscr = None
    monthly_payment = None
    if ask_price and stated_noi:
        ltv = 0.75
        loan_amount = ask_price * ltv
        rate_monthly = 0.07 / 12
        n_payments = 25 * 12
        try:
            monthly_payment = loan_amount * (rate_monthly * (1 + rate_monthly) ** n_payments) / \
                              ((1 + rate_monthly) ** n_payments - 1)
            annual_debt_service = monthly_payment * 12
            dscr = round(stated_noi / annual_debt_service, 2)
        except Exception:
            pass

    # Cash-on-cash at 25% down
    coc = None
    if stated_noi and ask_price and monthly_payment:
        equity = ask_price * 0.25
        annual_cf = stated_noi - (monthly_payment * 12)
        coc = round((annual_cf / equity) * 100, 2) if equity > 0 else None

    # Gross Rent Multiplier
    grm = None
    # (Would need actual rent roll — skip for now)

    analysis = {
        "source": "PropIntel Deal Analyzer",
        "based_on": "Stated listing cap rate + asking price — NOT verified rent roll",
        "note": "Verify with actual rent roll and T-12 income statement before making offers.",
        "asking_price": ask_price,
        "asking_price_fmt": listing.get("asking_price_fmt") or (
            "$" + f"{ask_price:,.0f}" if ask_price else None
        ),
        "price_per_sf": price_per_sf,
        "building_sf": bldg_sf,
        "stated_cap_rate": cap_rate_str,
        "stated_noi": stated_noi,
        "stated_noi_fmt": ("$" + f"{stated_noi:,}") if stated_noi else None,
        "assessed_total": assessed_total,
        "assessed_vs_asking_premium_pct": assessed_premium,
        "loan_assumptions": {
            "ltv_pct": 75,
            "rate_pct": 7.0,
            "amortization_years": 25,
            "down_payment_pct": 25,
        },
        "dscr": dscr,
        "cash_on_cash_pct": coc,
        "monthly_debt_service": round(monthly_payment) if monthly_payment else None,
    }

    # LOI targets (10% discount for initial offer)
    if ask_price:
        analysis["loi_targets"] = {
            "aggressive": round(ask_price * 0.88 / 1000) * 1000,
            "moderate": round(ask_price * 0.93 / 1000) * 1000,
            "conservative": round(ask_price * 0.97 / 1000) * 1000,
        }

    return analysis


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

    # Absentee / out-of-state owner
    if parcel.get("out_of_state_owner"):
        flags.append({"type": "yellow", "text": "⚠️ Out-of-state owner — mailing address outside TX (DCAD)"})
    elif parcel.get("absentee_owner"):
        owner_city = parcel.get("owner_city", "")
        flags.append({"type": "yellow", "text": f"Absentee owner — mailing address in {owner_city}, not at subject property (DCAD)"})

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
