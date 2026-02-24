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
from scrapers.dcad import search_by_address as dcad_by_address
from scrapers.regrid import search_by_point as regrid_by_point, search_by_address as regrid_by_address, search_nearby as regrid_nearby
from scrapers.txsos import search_by_address as txsos_address, search_entity as txsos_entity
from scrapers.listing import parse_listing, is_address, detect_source
from scrapers.datazapp import parse_owner_name
from scrapers import pdl as pdl_skip
from scrapers.walkscore import get_scores as walkscore_get
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
    with ThreadPoolExecutor(max_workers=5) as ex:
        if lat and lng:
            tasks["fema"] = ex.submit(get_flood_zone, lat, lng)
        if zip_code:
            tasks["census"] = ex.submit(get_demographics, zip_code)

        # Parcel lookup: Regrid by address (primary — most reliable), point fallback
        # Note: Census geocoder lat/lng not precise enough for parcel centroid matching
        # Note: trial key returns 403 with path= filter — use raw address only
        if address:
            tasks["regrid"] = ex.submit(regrid_by_address, address)
        elif lat and lng:
            tasks["regrid"] = ex.submit(regrid_by_point, lat, lng)

        # Nearby comparables via Regrid bounding box (free, same API key)
        if lat and lng:
            tasks["nearby"] = ex.submit(regrid_nearby, lat, lng, 0.3, 8)

        # Walk Score (free API — activates when WALKSCORE_API_KEY env var set)
        if lat and lng and address:
            tasks["walkscore"] = ex.submit(walkscore_get, address, lat, lng)

        # DCAD supplemental fetch for TX addresses (Dallas County extra fields: tax district, school, etc.)
        # geocode doesn't always return county — use state + zip prefix as proxy
        geo_state = geo.get("state", "").upper()
        geo_zip = geo.get("zip", "")
        is_dallas_county_likely = (
            geo_state == "TX" and
            any(geo_zip.startswith(p) for p in ("750", "751", "752"))
        )
        if address and is_dallas_county_likely:
            tasks["dcad"] = ex.submit(dcad_by_address, address)

        if address and tier == "pro":
            # Parse just the street number + name for TX SOS search
            street_match = re.match(r"(\d+\s+[\w\s]+?)(?:,|\s+\w{2}\s+\d{5})", address)
            street = street_match.group(1).strip() if street_match else address
            city = geo.get("city", "")
            tasks["txsos"] = ex.submit(txsos_address, street, city)

        # ── ATTOM enrichment (activates when ATTOM_API_KEY env var is set) ────
        import os as _os
        if _os.environ.get("ATTOM_API_KEY"):
            from scrapers.attom import get_avm, get_sold_comps, get_mortgage_lien, get_ownership_history
            attom_addr  = address.split(",")[0].strip() if address else ""
            attom_zip   = zip_code
            tasks["attom_avm"]      = ex.submit(get_avm, attom_addr, attom_zip)
            tasks["attom_comps"]    = ex.submit(get_sold_comps, attom_addr, attom_zip,
                                                 0.5, 12, 15)   # 0.5mi, 12mo, 15 comps
            tasks["attom_mortgage"] = ex.submit(get_mortgage_lien, attom_addr, attom_zip)
            tasks["attom_history"]  = ex.submit(get_ownership_history, attom_addr, attom_zip)

        # ── Realie enrichment (activates when REALIE_API_KEY set; ATTOM takes priority) ──
        elif _os.environ.get("REALIE_API_KEY"):
            from scrapers.realie import (
                get_avm as realie_avm,
                get_sold_comps as realie_comps,
                get_ownership_history as realie_history,
                get_mortgage_lien as realie_mortgage,
                get_property_detail as realie_detail,
            )
            _lat = geo.get("lat")
            _lng = geo.get("lng")
            tasks["realie_detail"]   = ex.submit(realie_detail, address)
            tasks["realie_avm"]      = ex.submit(realie_avm, address, _lat, _lng)
            tasks["realie_comps"]    = ex.submit(realie_comps, address, zip_code,
                                                  _lat, _lng, 1.0, 18, 15)
            tasks["realie_history"]  = ex.submit(realie_history, address)
            tasks["realie_mortgage"] = ex.submit(realie_mortgage, address)

        import concurrent.futures as _cf
        results = {}
        for key, future in tasks.items():
            try:
                results[key] = future.result(timeout=30)
            except _cf.TimeoutError:
                results[key] = {"error": "timeout", "warning": "Request timed out (30s)"}
                report["errors"].append("{}: timeout".format(key))
            except Exception as e:
                err_msg = str(e) or repr(e)
                results[key] = {"error": err_msg}
                report["errors"].append("{}: {}".format(key, err_msg))

    # ── Merge parcel data: Regrid primary, DCAD overlay for Dallas County ─────
    regrid_data = results.get("regrid", {})
    dcad_data = results.get("dcad", {})
    parcel_data = _merge_parcel(regrid_data, dcad_data, address)

    report["parcel"]       = parcel_data
    report["flood"]        = results.get("fema", {})
    report["demographics"] = results.get("census", {})
    report["businesses"]   = results.get("txsos", [])
    report["nearby"]       = [p for p in (results.get("nearby") or []) if p and not p.get("error")]
    report["walkscore"]    = results.get("walkscore", {"available": False})

    # ── Property enrichment: ATTOM (priority) or Realie (fallback) ──────────
    attom_avm      = results.get("attom_avm", {})
    attom_comps    = results.get("attom_comps", {})
    attom_mortgage = results.get("attom_mortgage", {})
    attom_history  = results.get("attom_history", {})

    realie_avm_r      = results.get("realie_avm", {})
    realie_comps_r    = results.get("realie_comps", {})
    realie_hist_r     = results.get("realie_history", {})
    realie_mortgage_r = results.get("realie_mortgage", {})

    # Merge: prefer ATTOM when available, fall back to Realie
    _avm      = attom_avm      if attom_avm.get("available")      else realie_avm_r
    _comps    = attom_comps    if attom_comps.get("available")    else realie_comps_r
    _mortgage = attom_mortgage if attom_mortgage.get("available") else realie_mortgage_r
    _history  = attom_history  if attom_history.get("available")  else realie_hist_r

    report["avm"]              = _avm      if _avm.get("available")     else {"available": False}
    report["sold_comps"]       = _comps    if _comps.get("available")   else {"available": False, "comps": []}
    report["mortgage"]         = _mortgage if _mortgage.get("available") else {"available": False}
    report["ownership_history"]= _history  if _history.get("available")  else {"available": False, "history": []}

    # If AVM is available (from either source), improve market estimate
    if _avm.get("available") and _avm.get("value"):
        _avm_source = _avm.get("source", "AVM")
        report["market_estimate"] = {
            "available":   True,
            "assessed":    parcel_data.get("assessed_total"),
            "assessed_fmt":f"${parcel_data.get('assessed_total',0):,.0f}" if parcel_data.get("assessed_total") else None,
            "market_low":  _avm.get("value_low") or _avm.get("value"),
            "market_high": _avm.get("value_high") or _avm.get("value"),
            "market_mid":  _avm.get("value"),
            "range_fmt":   _avm.get("range_fmt") or _avm.get("value_fmt"),
            "confidence":  f"{_avm.get('confidence_score')}%" if _avm.get("confidence_score") else _avm_source,
            "methodology": f"{_avm_source} Automated Valuation Model (AVM)",
            "source":      _avm_source,
            "avm_date":    _avm.get("calc_date"),
        }

    # ── Owner entity intelligence (all tiers — public TX SOS data) ────────────
    owner_name = parcel_data.get("owner_name", "")
    if owner_name:
        _, _, is_entity = parse_owner_name(owner_name)
        if is_entity:
            try:
                entity_data = txsos_entity(owner_name)
                report["owner_entity"] = entity_data
            except Exception as e:
                report["owner_entity"] = {"error": str(e), "entity_name": owner_name}
        else:
            report["owner_entity"] = {"is_individual": True, "entity_name": owner_name}

    # ── Market value estimate (ATTOM AVM if available, else assessed-based) ───
    if not report.get("market_estimate", {}).get("available"):
        report["market_estimate"] = _estimate_market_value(parcel_data)

    # ── Financial estimates (tax, cash flow, cap rate — all tiers) ───────────
    report["financials"] = _estimate_financials(parcel_data, report["market_estimate"])

    # ── Permit portal link ────────────────────────────────────────────────────
    city_name = (geo.get("city") or "").strip().upper()
    report["permit_portal"] = _get_permit_portal(city_name, geo.get("state", ""))

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

        # Extract hold_years from Realie/ATTOM ownership history when available.
        # report["ownership_history"] is set earlier from either source.
        _ownership = report.get("ownership_history", {})
        _hold_years = None
        if isinstance(_ownership, dict) and _ownership.get("available"):
            _hold_years = _ownership.get("hold_years")
            # Some sources nest it under the first history entry
            if _hold_years is None:
                _hist_list = _ownership.get("history", [])
                if _hist_list and isinstance(_hist_list[0], dict):
                    _hold_years = _hist_list[0].get("hold_years")
        # Validate: must be a non-negative number
        if _hold_years is not None:
            try:
                _hold_years = float(_hold_years)
                if _hold_years < 0:
                    _hold_years = None
            except (TypeError, ValueError):
                _hold_years = None

        report["motivation"] = motivation_score(
            parcel=parcel_data,
            listing=listing_meta,
            deed_history=parcel_data.get("deed_history", []),
            hold_years=_hold_years,
        )

        # DataZapp skip trace
        first, last, is_entity = parse_owner_name(owner_name)
        owner_mail_addr = parcel_data.get("owner_mailing", "")
        owner_city = parcel_data.get("owner_city", "")
        owner_state = parcel_data.get("owner_state", "TX")
        owner_zip = parcel_data.get("owner_zip", "")

        if is_entity:
            # Entities → PDL entity fallback (PDL is person-focused; returns a note)
            report["skip_trace"] = pdl_skip.skip_trace_entity(
                entity_name=owner_name,
                mailing_address=owner_mail_addr,
                mailing_city=owner_city,
                mailing_state=owner_state,
                mailing_zip=owner_zip,
            )
        elif first or last:
            # Individual owner → PDL person enrichment
            report["skip_trace"] = pdl_skip.skip_trace(
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
                "source": "People Data Labs",
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


def _get_permit_portal(city: str, state: str) -> dict:
    """Return permit portal + assessor URLs for known TX cities."""
    TX_PORTALS = {
        "DALLAS":        {"permit": "https://developdallas.dallascityhall.com/", "assessor": "https://www.dallascad.org/"},
        "FORT WORTH":    {"permit": "https://fortworthtexas.gov/departments/development-services/permits", "assessor": "https://www.tad.org/"},
        "GARLAND":       {"permit": "https://www.garlandtx.gov/175/Permits", "assessor": "https://www.dallascad.org/"},
        "PLANO":         {"permit": "https://www.plano.gov/1226/Permits", "assessor": "https://www.collincad.org/"},
        "IRVING":        {"permit": "https://www.cityofirving.org/government/departments/development-services", "assessor": "https://www.dallascad.org/"},
        "ARLINGTON":     {"permit": "https://www.arlingtontx.gov/city_hall/departments/planning_development_services", "assessor": "https://www.tad.org/"},
        "FRISCO":        {"permit": "https://www.friscotexas.gov/1003/Permits", "assessor": "https://www.collincad.org/"},
        "MCKINNEY":      {"permit": "https://www.mckinneytexas.org/permits", "assessor": "https://www.collincad.org/"},
        "MESQUITE":      {"permit": "https://www.cityofmesquite.com/299/Permits", "assessor": "https://www.dallascad.org/"},
        "RICHARDSON":    {"permit": "https://www.cor.net/departments/development-services", "assessor": "https://www.dallascad.org/"},
        "CARROLLTON":    {"permit": "https://www.cityofcarrollton.com/government/departments/development-services", "assessor": "https://www.dallascad.org/"},
        "KAUFMAN":       {"permit": "https://www.kaufmantx.org/", "assessor": "https://www.kaufmancad.org/"},
        "TERRELL":       {"permit": "https://www.terrellonline.com/", "assessor": "https://www.kaufmancad.org/"},
        "HOUSTON":       {"permit": "https://www.houstonpermittingcenter.org/", "assessor": "https://hcad.org/"},
        "AUSTIN":        {"permit": "https://austintexas.gov/permits", "assessor": "https://traviscad.org/"},
        "SAN ANTONIO":   {"permit": "https://saonlinepermits.sanantonio.gov/", "assessor": "https://bexar.org/"},
    }
    portal = TX_PORTALS.get(city)
    if portal:
        return {"city": city.title(), "state": state, **portal}
    # Generic fallback
    return {
        "city": city.title(), "state": state,
        "permit": None,
        "assessor": None,
        "note": "Search '{}  building permits' for city portal".format(city.title()),
    }


def _estimate_market_value(parcel: dict) -> dict:
    """
    Estimate market value range from county assessed value.

    TX assessed values are often 75-90% of market for residential,
    85-100% for commercial. We apply conservative multipliers and
    clearly flag as ESTIMATE, not appraisal.
    """
    assessed = parcel.get("assessed_total")
    if not assessed or assessed <= 0:
        return {"available": False, "note": "Assessed value required for estimate"}

    use_desc = (parcel.get("use_description") or "").upper()
    prop_class = (parcel.get("property_class") or "").upper()

    # Classify property type
    is_commercial = any(w in use_desc for w in ("COMMERCIAL", "OFFICE", "RETAIL", "INDUSTRIAL", "WAREHOUSE", "MULTI"))
    is_land = any(w in use_desc for w in ("VACANT", "LAND", "AGRICULTURAL", "FARM", "RANCH"))
    is_residential = not is_commercial and not is_land

    # TX market multipliers (conservative, calibrated for DFW market)
    if is_commercial:
        lo_mult, hi_mult = 1.05, 1.25  # Commercial often closer to assessed
        type_label = "Commercial"
    elif is_land:
        lo_mult, hi_mult = 0.90, 1.30  # Land values volatile
        type_label = "Land / Vacant"
    else:
        lo_mult, hi_mult = 1.08, 1.22  # Residential: DFW appreciation
        type_label = "Residential"

    market_low = round(assessed * lo_mult / 1000) * 1000   # Round to nearest $1K
    market_high = round(assessed * hi_mult / 1000) * 1000

    def fmt(v):
        if v >= 1_000_000:
            return "${:.2f}M".format(v / 1_000_000)
        return "${:,.0f}".format(v)

    return {
        "available": True,
        "assessed": assessed,
        "assessed_fmt": fmt(assessed),
        "market_low": market_low,
        "market_high": market_high,
        "range_fmt": "{} – {}".format(fmt(market_low), fmt(market_high)),
        "property_type": type_label,
        "confidence": "Moderate",
        "methodology": "County assessed value × DFW market multiplier ({:.0f}%–{:.0f}%). "
                       "Not an appraisal. Verify with recent comparable sales.".format(
                           lo_mult * 100, hi_mult * 100),
        "note": "Assessed value is the county tax value, typically below market. "
                "This range is a data-driven estimate only.",
    }


def _estimate_financials(parcel: dict, market_est: dict) -> dict:
    """
    Estimate annual property tax, potential cash flow, and holding cost.
    All estimates — clearly labeled. Based on DFW average rates.
    """
    assessed = parcel.get("assessed_total")
    if not assessed or assessed <= 0:
        return {"available": False}

    use_desc = (parcel.get("use_description") or "").upper()
    building_sf = None
    try:
        building_sf = float(parcel.get("building_sf") or 0)
    except Exception:
        pass

    # TX property tax rates (effective combined rate, DFW average 2025)
    is_commercial = any(w in use_desc for w in ("COMMERCIAL", "OFFICE", "RETAIL", "INDUSTRIAL", "WAREHOUSE", "MULTI"))
    is_residential = not is_commercial

    tax_rate = 0.0195 if is_commercial else 0.0225   # ~2.25% residential, ~1.95% commercial in Dallas County
    est_annual_tax = round(assessed * tax_rate)
    est_monthly_tax = round(est_annual_tax / 12)

    result = {
        "available": True,
        "est_annual_tax": est_annual_tax,
        "est_monthly_tax": est_monthly_tax,
        "tax_rate_pct": round(tax_rate * 100, 2),
        "tax_note": "Estimate based on Dallas County effective rate. Actual varies by tax district.",
    }

    # Cash flow potential (commercial only, if we have building SF)
    if is_commercial and building_sf and building_sf > 0:
        # DFW commercial rental rates 2025 (low / mid / high per SF/year)
        if any(w in use_desc for w in ("INDUSTRIAL", "WAREHOUSE", "DISTRIBUTION")):
            rent_low, rent_mid, rent_high = 6.0, 8.5, 11.0
            use_label = "Industrial/Warehouse"
        elif any(w in use_desc for w in ("RETAIL", "STRIP", "SHOPPING")):
            rent_low, rent_mid, rent_high = 14.0, 20.0, 28.0
            use_label = "Retail"
        elif any(w in use_desc for w in ("OFFICE",)):
            rent_low, rent_mid, rent_high = 16.0, 22.0, 30.0
            use_label = "Office"
        else:
            rent_low, rent_mid, rent_high = 12.0, 18.0, 25.0
            use_label = "Commercial"

        gsi_low  = round(building_sf * rent_low)
        gsi_mid  = round(building_sf * rent_mid)
        gsi_high = round(building_sf * rent_high)
        # NOI after ~35% expenses (tax + insurance + maintenance + vacancy)
        noi_low  = round(gsi_low  * 0.65)
        noi_mid  = round(gsi_mid  * 0.65)
        noi_high = round(gsi_high * 0.65)

        def fmtd(v): return "${:,.0f}".format(v)

        result.update({
            "cash_flow": True,
            "rent_per_sf_range": "${:.0f} – ${:.0f}".format(rent_low, rent_high),
            "gsi_range": "{} – {}".format(fmtd(gsi_low), fmtd(gsi_high)),
            "noi_range": "{} – {}".format(fmtd(noi_low), fmtd(noi_high)),
            "rent_use_label": use_label,
            "building_sf": int(building_sf),
            "cash_flow_note": "DFW market rent estimate. 65% expense ratio applied. Verify with current lease comps.",
        })

        # Cap rate implied by market estimate
        if market_est.get("available"):
            mkt_mid = (market_est["market_low"] + market_est["market_high"]) / 2
            if mkt_mid > 0:
                implied_cap = round(noi_mid / mkt_mid * 100, 2)
                result["implied_cap_rate"] = implied_cap

    return result


def _merge_parcel(regrid: dict, dcad: dict, address: str) -> dict:
    """
    Merge Regrid (primary, national) + DCAD (supplemental, Dallas County only).

    Strategy:
    - Use Regrid as base if it returned valid data (owner_name or apn present)
    - Overlay DCAD fields that Regrid doesn't have (tax_district, school_district, etc.)
    - Fall back to DCAD entirely if Regrid failed or returned outside-coverage error
    - Fall back to address-only stub if both fail
    """
    regrid_ok = regrid and not regrid.get("error") and (regrid.get("owner_name") or regrid.get("apn"))
    dcad_ok = dcad and not dcad.get("error") and (dcad.get("owner_name") or dcad.get("apn"))

    if regrid_ok:
        merged = dict(regrid)
        # Overlay DCAD-specific fields that Regrid lacks
        if dcad_ok:
            for field in ("tax_district", "school_district", "use_code",
                          "legal_description", "subdivision", "revalue_year",
                          "assessed_yoy_pct", "dcad_direct_url"):
                if dcad.get(field) and not merged.get(field):
                    merged[field] = dcad[field]
            # DCAD assessed values are often more up-to-date for TX
            if dcad.get("assessed_total") and not merged.get("assessed_total"):
                merged["assessed_total"] = dcad["assessed_total"]
                merged["assessed_land"] = dcad.get("assessed_land")
                merged["assessed_improvement"] = dcad.get("assessed_improvement")
            # Tax delinquency from DCAD
            if dcad.get("tax_delinquent"):
                merged["tax_delinquent"] = True
        merged["data_sources"] = "Regrid" + (" + DCAD" if dcad_ok else "")
        return merged

    if dcad_ok:
        dcad["data_sources"] = "DCAD"
        return dcad

    # Both failed — return structured error (never silently return N/A)
    regrid_err = regrid.get("error", "unknown") if regrid else "no response"
    regrid_err_type = regrid.get("error_type", "") if regrid else ""
    dcad_err = dcad.get("warning") or dcad.get("error", "") if dcad else ""

    # Human-readable reason for the report UI
    if "expired" in regrid_err.lower() or "auth" in regrid_err_type:
        user_reason = "Parcel data service key expired. Contact PropIntel support."
        action = "renew_key"
    elif "coverage" in regrid_err_type or "outside" in regrid_err.lower():
        user_reason = "This address is currently outside our covered service area. We're expanding coverage — check back soon."
        action = "outside_coverage"
    elif "timeout" in regrid_err.lower():
        user_reason = "Parcel data request timed out. Please try again in a moment."
        action = "retry"
    else:
        user_reason = "Parcel data temporarily unavailable. Please try again."
        action = "retry"

    fallback = {
        "source": "PropIntel",
        "parcel_error": True,
        "parcel_error_reason": user_reason,
        "parcel_error_action": action,
        "warning": "Parcel data unavailable — {}".format(regrid_err),
        "dcad_warning": dcad_err or None,
        "data_sources": "none",
    }
    # Include DCAD manual link if available
    if dcad and dcad.get("manual_url"):
        fallback["manual_url"] = dcad["manual_url"]
    return fallback


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
