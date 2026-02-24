"""
Collin Central Appraisal District (Collin CAD) — Property lookup via TrueAutomation.
Covers: Frisco, McKinney, Allen, Plano (Collin portion), Prosper, Celina, Wylie, etc.

TrueAutomation portal: https://propaccess.trueautomation.com/clientdb/?cid=60
Public REST API — no auth required.

Endpoints:
  Search: GET /clientdb/Property/SearchByAddress?cid=60&term=<address>&limit=10
  Detail: GET /clientdb/Property/PropertyDetail?cid=60&prop_id=<id>
"""

import json
import logging
import urllib.request
import urllib.parse
import urllib.error

log = logging.getLogger(__name__)

BASE_URL = "https://propaccess.trueautomation.com/clientdb"
CID = 60
SOURCE = "Collin Central Appraisal District"
SOURCE_URL = "https://propaccess.trueautomation.com/clientdb/?cid=60"
TIMEOUT = 12

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://propaccess.trueautomation.com/clientdb/?cid=60",
}


def _get(path, params):
    """Make a GET request to TrueAutomation and return parsed JSON or error dict."""
    qs = urllib.parse.urlencode(params)
    url = "{}/{}?{}".format(BASE_URL, path.lstrip("/"), qs)
    req = urllib.request.Request(url, headers=HEADERS, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
            body = resp.read().decode("utf-8", errors="ignore")
            return json.loads(body)
    except urllib.error.HTTPError as e:
        body = ""
        try:
            body = e.read().decode("utf-8", errors="ignore")[:200]
        except Exception:
            pass
        log.warning("Collin CAD HTTP %s on %s: %s", e.code, path, body)
        return {"error": "HTTP {}".format(e.code), "detail": body}
    except urllib.error.URLError as e:
        log.warning("Collin CAD URLError on %s: %s", path, e.reason)
        return {"error": "Connection error: {}".format(str(e.reason))}
    except Exception as e:
        log.warning("Collin CAD error on %s: %s", path, e)
        return {"error": str(e) or repr(e)}


def _search_by_address(address):
    """Search TrueAutomation for properties matching the given address string."""
    params = {
        "cid": CID,
        "term": address,
        "limit": 10,
    }
    return _get("Property/SearchByAddress", params)


def _get_property_detail(prop_id):
    """Fetch full property detail by TrueAutomation prop_id."""
    params = {
        "cid": CID,
        "prop_id": prop_id,
    }
    return _get("Property/PropertyDetail", params)


def _safe_float(val):
    """Convert value to float, return None on failure."""
    if val is None:
        return None
    try:
        return float(str(val).replace(",", "").strip())
    except (ValueError, TypeError):
        return None


def _safe_int(val):
    """Convert value to int, return None on failure."""
    f = _safe_float(val)
    if f is None:
        return None
    try:
        return int(f)
    except (ValueError, TypeError):
        return None


def _parse_owner(detail):
    """
    Extract owner name and mailing address from property detail.
    TrueAutomation 'owners' is a list; take the first entry.
    """
    owners = detail.get("owners") or detail.get("owner") or []
    if isinstance(owners, dict):
        owners = [owners]
    if not owners:
        return "", None, "", "", "", ""

    o = owners[0]
    owner_name = (o.get("name") or o.get("ownerName") or "").strip()

    # Mailing address fields vary by cid
    mail_addr = (o.get("addr1") or o.get("mailingAddress") or o.get("address1") or "").strip()
    mail_city = (o.get("city") or o.get("mailingCity") or "").strip()
    mail_state = (o.get("state") or o.get("mailingState") or "").strip()
    mail_zip = (o.get("zip") or o.get("mailingZip") or "").strip()

    mailing = mail_addr or None
    if mailing and mail_city:
        mailing = "{}, {}".format(mailing, mail_city)
    if mailing and mail_state:
        mailing = "{} {}".format(mailing, mail_state)
    if mailing and mail_zip:
        mailing = "{} {}".format(mailing, mail_zip)

    return owner_name, mailing, mail_city, mail_state, mail_zip, ""


def _parse_values(detail):
    """
    Extract assessed and market values from TrueAutomation property detail.
    'values' is typically a list of year-keyed dicts.
    """
    values_list = detail.get("values") or detail.get("appraisalInfo") or []
    if isinstance(values_list, dict):
        values_list = [values_list]

    if not values_list:
        return None, None, None, None

    # Take the most recent year (last entry or largest year)
    best = None
    best_year = -1
    for v in values_list:
        yr = _safe_int(v.get("taxyear") or v.get("year") or v.get("taxYear") or 0)
        if yr is not None and yr > best_year:
            best_year = yr
            best = v

    if not best:
        best = values_list[-1]

    # Field names vary; try multiple
    total = _safe_float(
        best.get("totalAppraisedValue") or
        best.get("appraisedValue") or
        best.get("totalValue") or
        best.get("marketValue")
    )
    land = _safe_float(
        best.get("landValue") or
        best.get("landAppraisedValue")
    )
    improvement = _safe_float(
        best.get("improvementValue") or
        best.get("buildingValue") or
        best.get("improvAppraisedValue")
    )

    # Derive missing component
    if total and land and improvement is None:
        improvement = total - land
    elif total and improvement and land is None:
        land = total - improvement

    assessed = _safe_float(
        best.get("assessedValue") or
        best.get("totalAssessedValue") or
        best.get("netAppraisedValue") or
        total
    )

    return assessed, land, improvement, best_year if best_year > 0 else None


def _parse_improvements(detail):
    """Extract building area, year built from improvements list."""
    improvements = detail.get("improvements") or detail.get("improvement") or []
    if isinstance(improvements, dict):
        improvements = [improvements]

    if not improvements:
        return None, None

    # Sum building areas across all improvement records
    total_sf = 0.0
    year_built = None
    for imp in improvements:
        sf = _safe_float(
            imp.get("livingArea") or
            imp.get("buildingArea") or
            imp.get("squareFeet") or
            imp.get("area")
        )
        if sf and sf > 0:
            total_sf += sf
        yr = _safe_int(
            imp.get("yearBuilt") or
            imp.get("effectiveYear")
        )
        if yr and yr > 1800:
            year_built = yr

    building_sf = int(total_sf) if total_sf > 0 else None
    return building_sf, year_built


def _parse_detail(detail, address):
    """Convert a TrueAutomation PropertyDetail dict to PropIntel parcel format."""
    prop_id = detail.get("prop_id") or detail.get("propId") or ""
    apn = (
        detail.get("geo_id") or
        detail.get("geoId") or
        detail.get("parcelId") or
        str(prop_id)
    ).strip()

    situs = (
        detail.get("situs_address") or
        detail.get("situsAddress") or
        detail.get("propertyAddress") or
        address or
        ""
    ).strip()

    legal = (
        detail.get("legal_desc") or
        detail.get("legalDescription") or
        detail.get("legalDesc") or
        ""
    ).strip()

    subdivision = (
        detail.get("subdivision") or
        detail.get("subdiv") or
        ""
    ).strip() or None

    use_desc = (
        detail.get("stateClassDescription") or
        detail.get("propertyUseDescription") or
        detail.get("land_state_cd_description") or
        detail.get("useDescription") or
        ""
    ).strip()

    use_code = (
        detail.get("stateCode") or
        detail.get("propertyUseCode") or
        detail.get("land_state_cd") or
        ""
    )

    school = (
        detail.get("schoolDistrictName") or
        detail.get("school_dist_name") or
        ""
    ).strip()

    owner_name, mailing, mail_city, mail_state, mail_zip, _ = _parse_owner(detail)

    assessed, land_val, improvement_val, revalue_year = _parse_values(detail)
    building_sf, year_built = _parse_improvements(detail)

    # Absentee / out-of-state detection
    out_of_state = (mail_state.upper() not in ("TX", "")) if mail_state else False
    # Absentee: owner mailing city differs from situs city
    situs_city = (
        detail.get("situs_city") or
        detail.get("situsCity") or
        ""
    ).strip().upper()
    absentee = (
        bool(mail_city) and
        bool(situs_city) and
        mail_city.upper() != situs_city
    )

    result = {
        "source": SOURCE,
        "source_url": SOURCE_URL,
        "apn": apn,
        "prop_id": str(prop_id),
        "owner_name": owner_name,
        "owner_name2": None,
        "owner_mailing": mailing,
        "owner_city": mail_city,
        "owner_state": mail_state,
        "owner_zip": mail_zip,
        "property_address": situs,
        "legal_description": legal,
        "subdivision": subdivision,
        "use_code": str(use_code) if use_code else None,
        "use_description": use_desc,
        "property_class": use_desc,
        "tax_district": None,
        "school_district": school or None,
        "building_sf": building_sf,
        "year_built": year_built,
        "assessed_total": assessed,
        "assessed_land": land_val,
        "assessed_improvement": improvement_val,
        "assessed_prev": None,
        "assessed_yoy_pct": None,
        "revalue_year": revalue_year,
        "absentee_owner": absentee,
        "out_of_state_owner": out_of_state,
        "tax_delinquent": False,
        "county": "Collin County",
        "state": "TX",
        "collin_cad_url": "{}/?cid={}&prop_id={}".format(BASE_URL, CID, prop_id),
    }
    return result


def search_by_address(address):
    """
    Search Collin CAD by property address using TrueAutomation REST API.
    Returns PropIntel-compatible parcel dict or fallback with manual link.
    """
    if not address:
        return _fallback(address, "No address provided")

    # Use just the street portion for best match (strip city/state/zip)
    street = address.split(",")[0].strip()

    search_result = _search_by_address(street)
    if search_result.get("error"):
        return _fallback(address, "Search error: {}".format(search_result["error"]))

    # Results may be in different keys depending on API version
    properties = (
        search_result.get("data") or
        search_result.get("properties") or
        search_result.get("results") or
        (search_result if isinstance(search_result, list) else [])
    )

    if not properties:
        return _fallback(address, "No parcel found for this address in Collin CAD")

    first = properties[0] if isinstance(properties, list) else properties

    # Get prop_id for detail call
    prop_id = (
        first.get("prop_id") or
        first.get("propId") or
        first.get("id") or
        ""
    )

    if not prop_id:
        # Try to parse directly from search result if it already has owner/value data
        return _parse_detail(first, address)

    detail = _get_property_detail(prop_id)
    if detail.get("error"):
        # Parse what we got from the search result
        return _parse_detail(first, address)

    # Detail may be wrapped in a key
    if isinstance(detail, dict) and "data" in detail:
        detail = detail["data"]
    if isinstance(detail, list) and detail:
        detail = detail[0]

    return _parse_detail(detail, address)


def _fallback(query, reason):
    """Structured fallback when Collin CAD lookup fails."""
    encoded = urllib.parse.quote(str(query or ""))
    return {
        "source": SOURCE,
        "source_url": SOURCE_URL,
        "warning": reason,
        "manual_url": "{}/?cid={}&SearchValue={}".format(BASE_URL, CID, encoded),
        "note": "Direct Collin CAD lookup required. Use the link to retrieve owner, APN, and assessed value.",
        "available": False,
    }
