"""
County Router — routes parcel data requests to the correct scraper.

Coverage cascade (in order):
  Tier 1 — Direct county ArcGIS scrapers (TX majors): instant, free, authoritative
  Tier 2 — RentCast API (140M properties nationwide): set RENTCAST_API_KEY
  Tier 3 — Realie property search: national but patchy coverage
  Tier 4 — Graceful fallback with manual link
"""

# County name (lowercased, ' county' stripped) → scraper module name
COUNTY_MAP = {
    # Texas
    'dallas': 'dcad',
    'harris': 'hcad',
    'bexar': 'bcad',
    'travis': 'tcad',
    'tarrant': 'tarcad',
    'collin': 'cad_collin',
    'denton': 'cad_denton',
    # Add more as scrapers are built
}

# City name (lowercased) → scraper module name
CITY_MAP = {
    # Dallas County TX
    'dallas': 'dcad',
    'irving': 'dcad',
    'garland': 'dcad',
    'richardson': 'dcad',
    'mesquite': 'dcad',
    'carrollton': 'dcad',
    'desoto': 'dcad',
    'duncanville': 'dcad',
    'cedar hill': 'dcad',
    'lancaster': 'dcad',
    'grand prairie': 'dcad',
    'farmers branch': 'dcad',
    'coppell': 'dcad',
    'balch springs': 'dcad',
    'rowlett': 'dcad',
    'sunnyvale': 'dcad',
    'seagoville': 'dcad',
    'hutchins': 'dcad',
    'wilmer': 'dcad',
    'sachse': 'dcad',

    # Harris County TX
    'houston': 'hcad',
    'pasadena': 'hcad',
    'pearland': 'hcad',
    'katy': 'hcad',
    'sugar land': 'hcad',
    'the woodlands': 'hcad',
    'baytown': 'hcad',
    'la porte': 'hcad',
    'humble': 'hcad',
    'league city': 'hcad',
    'friendswood': 'hcad',
    'deer park': 'hcad',
    'channelview': 'hcad',
    'cypress': 'hcad',
    'spring': 'hcad',
    'tomball': 'hcad',
    'stafford': 'hcad',
    'bellaire': 'hcad',
    'west university place': 'hcad',

    # Bexar County TX
    'san antonio': 'bcad',
    'leon valley': 'bcad',
    'converse': 'bcad',
    'selma': 'bcad',
    'live oak': 'bcad',
    'schertz': 'bcad',
    'universal city': 'bcad',
    'windcrest': 'bcad',
    'kirby': 'bcad',
    'alamo heights': 'bcad',
    'terrell hills': 'bcad',
    'shavano park': 'bcad',
    'hill country village': 'bcad',
    'olmos park': 'bcad',
    'castle hills': 'bcad',
    'balcones heights': 'bcad',

    # Travis County TX
    'austin': 'tcad',
    'cedar park': 'tcad',
    'round rock': 'tcad',
    'pflugerville': 'tcad',
    'manor': 'tcad',
    'bee cave': 'tcad',
    'lakeway': 'tcad',
    'rollingwood': 'tcad',
    'west lake hills': 'tcad',
    'sunset valley': 'tcad',

    # Tarrant County TX
    'fort worth': 'tarcad',
    'arlington': 'tarcad',
    'mansfield': 'tarcad',
    'euless': 'tarcad',
    'bedford': 'tarcad',
    'hurst': 'tarcad',
    'grapevine': 'tarcad',
    'southlake': 'tarcad',
    'keller': 'tarcad',
    'north richland hills': 'tarcad',
    'richland hills': 'tarcad',
    'colleyville': 'tarcad',
    'watauga': 'tarcad',
    'haltom city': 'tarcad',
    'forest hill': 'tarcad',
    'crowley': 'tarcad',
    'burleson': 'tarcad',
    'kennedale': 'tarcad',
    'azle': 'tarcad',
    'saginaw': 'tarcad',

    # Collin County TX
    'frisco': 'cad_collin',
    'mckinney': 'cad_collin',
    'allen': 'cad_collin',
    'plano': 'cad_collin',
    'prosper': 'cad_collin',
    'celina': 'cad_collin',
    'anna': 'cad_collin',
    'wylie': 'cad_collin',
    'murphy': 'cad_collin',
    'fairview': 'cad_collin',
    'lucas': 'cad_collin',
    'parker': 'cad_collin',
    'princeton': 'cad_collin',
    'nevada': 'cad_collin',
    'blue ridge': 'cad_collin',
    'farmersville': 'cad_collin',

    # Denton County TX
    'denton': 'cad_denton',
    'lewisville': 'cad_denton',
    'flower mound': 'cad_denton',
    'highland village': 'cad_denton',
    'little elm': 'cad_denton',
    'the colony': 'cad_denton',
    'corinth': 'cad_denton',
    'lake dallas': 'cad_denton',
    'hickory creek': 'cad_denton',
    'shady shores': 'cad_denton',
    'argyle': 'cad_denton',
    'bartonville': 'cad_denton',
    'lantana': 'cad_denton',
    'double oak': 'cad_denton',
    'trophy club': 'cad_denton',
    'roanoke': 'cad_denton',
    'westlake': 'cad_denton',
    'aubrey': 'cad_denton',
    'krugerville': 'cad_denton',
}


def detect_scraper(geo):
    """
    Returns scraper module name (str) or None if no direct scraper available.

    Args:
        geo: dict with optional 'county' and 'city' keys
    """
    county = (geo.get('county') or '').lower().replace(' county', '').strip()
    city = (geo.get('city') or '').lower().strip()

    if county and county in COUNTY_MAP:
        return COUNTY_MAP[county]
    if city and city in CITY_MAP:
        return CITY_MAP[city]
    return None


def get_parcel_data(address, geo):
    """
    Route parcel lookup to the correct county scraper.

    Args:
        address: str — full property address
        geo: dict — geocoded data with 'city' and/or 'county' keys

    Returns:
        dict with parcel fields, or {"error": ..., "source": "none", "available": False}
        on no-coverage, or {"error": ..., "source": scraper_name} on scraper error.
    """
    # ── Tier 1: Direct county scraper ────────────────────────────────────────
    scraper_name = detect_scraper(geo)
    if scraper_name:
        try:
            import importlib
            module = importlib.import_module("scrapers.{}".format(scraper_name))
            fn = getattr(module, "search_by_address", None) or getattr(module, "search_by_point", None)
            if fn:
                result = fn(address)
                if isinstance(result, dict):
                    result.setdefault("source", scraper_name)
                # Return if we got real owner data
                if isinstance(result, dict) and result.get("owner_name"):
                    return result
        except Exception as e:
            pass  # Fall through to next tier

    # ── Tier 2: RentCast (national, 140M properties) ─────────────────────────
    try:
        from scrapers.rentcast import get_property, is_available
        if is_available():
            rc = get_property(address)
            if rc.get("available") and rc.get("owner_name"):
                return rc
    except Exception:
        pass

    # ── Tier 3: Realie property search (patchy but free) ─────────────────────
    try:
        from scrapers.realie import get_property_detail
        import re as _re
        state_match = _re.search(r",\s*([A-Z]{2})\s+\d{5}", address.upper())
        state = state_match.group(1) if state_match else "TX"
        realie = get_property_detail(address)
        if realie.get("available") and realie.get("owner_name"):
            return {
                "source": "realie",
                "owner_name": realie.get("owner_name"),
                "owner_mailing": None,
                "assessed_total": realie.get("assessed_total"),
                "building_sf": realie.get("building_sf"),
                "year_built": realie.get("year_built"),
                "bedrooms": realie.get("bedrooms"),
                "bathrooms": realie.get("bathrooms"),
                "use_description": realie.get("use_description"),
                "absentee_owner": False,
                "out_of_state_owner": False,
                "tax_delinquent": False,
            }
    except Exception:
        pass

    # ── Tier 4: Graceful fallback ─────────────────────────────────────────────
    encoded = address.replace(" ", "+") if address else ""
    county = (geo.get("county") or "").replace(" ", "+")
    state = (geo.get("state") or "")
    return {
        "error": "Parcel data not yet available for this county",
        "source": "none",
        "available": False,
        "parcel_error": True,
        "parcel_error_reason": "County parcel data coming soon. Comps, flood zone, and demographics are still available below.",
        "parcel_error_action": "info",
        "manual_url": "https://www.google.com/search?q={}+{}+{}+appraisal+district+property+search".format(
            encoded, county, state
        ),
    }
