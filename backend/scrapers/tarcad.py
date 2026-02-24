"""
Tarrant County Appraisal District (TARCAD) — Fort Worth, TX
Free property lookup via TrueAutomation public portal (no auth, no API key).
CID: 24

Endpoints:
  Search:  GET https://propaccess.trueautomation.com/clientdb/Property/SearchByAddress
  Detail:  GET https://propaccess.trueautomation.com/clientdb/Property/PropertyDetail
"""
import json
import urllib.request
import urllib.parse
import urllib.error

SOURCE = "tarcad"
COUNTY = "Tarrant"
STATE = "TX"
CID = 24
TIMEOUT = 12

BASE = "https://propaccess.trueautomation.com/clientdb/Property"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://propaccess.trueautomation.com/",
}


def _get_json(url):
    """Fetch JSON from url; returns (data, error_str)."""
    req = urllib.request.Request(url, headers=HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            return json.loads(raw), None
    except urllib.error.HTTPError as e:
        return None, "HTTP {}: {}".format(e.code, e.reason)
    except urllib.error.URLError as e:
        return None, "URLError: {}".format(str(e.reason))
    except Exception as e:
        return None, str(e) or repr(e)


def _search(term):
    """Search by address; returns list of result dicts or (None, error)."""
    params = urllib.parse.urlencode({"cid": CID, "term": term, "limit": 10})
    url = "{}/SearchByAddress?{}".format(BASE, params)
    data, err = _get_json(url)
    if err:
        return None, err
    if not isinstance(data, list):
        if isinstance(data, dict):
            data = data.get("results") or data.get("data") or []
        else:
            data = []
    return data, None


def _detail(prop_id):
    """Fetch full property detail; returns (data_dict, error)."""
    params = urllib.parse.urlencode({"cid": CID, "prop_id": prop_id})
    url = "{}/PropertyDetail?{}".format(BASE, params)
    data, err = _get_json(url)
    if err:
        return None, err
    if isinstance(data, list) and data:
        data = data[0]
    return data, None


def _safe_float(val):
    """Convert value to float, return 0.0 on failure."""
    try:
        return float(val) if val is not None else 0.0
    except (TypeError, ValueError):
        return 0.0


def _safe_int(val):
    """Convert value to int, return 0 on failure."""
    try:
        return int(val) if val is not None else 0
    except (TypeError, ValueError):
        return 0


def _parse(detail, search_hit=None):
    """
    Convert TrueAutomation PropertyDetail response into PropIntel parcel dict.
    """
    d = detail or {}
    sh = search_hit or {}

    owner_name = (
        d.get("ownerName") or d.get("owner_name") or
        sh.get("ownerName") or sh.get("owner_name") or ""
    ).strip()

    mailing_addr = (d.get("ownerAddress") or d.get("mailingAddress") or "").strip()
    mailing_city = (d.get("ownerCity") or d.get("mailingCity") or "").strip()
    mailing_state = (d.get("ownerState") or d.get("mailingState") or "").strip().upper()
    mailing_zip = (d.get("ownerZip") or d.get("mailingZip") or "").strip()

    owner_mailing = ""
    if mailing_addr:
        owner_mailing = mailing_addr
        if mailing_city:
            owner_mailing += ", " + mailing_city
        if mailing_state:
            owner_mailing += " " + mailing_state
        if mailing_zip:
            owner_mailing += " " + mailing_zip

    prop_addr = (
        d.get("propertyAddress") or d.get("siteAddress") or
        sh.get("propertyAddress") or sh.get("address") or ""
    ).strip()
    prop_city = (d.get("propertyCity") or d.get("siteCity") or "Fort Worth").strip()
    prop_zip = (d.get("propertyZip") or d.get("siteZip") or "").strip()

    apn = (
        d.get("propertyId") or d.get("prop_id") or d.get("apn") or d.get("accountNum") or
        sh.get("prop_id") or sh.get("propertyId") or ""
    )
    apn = str(apn).strip()

    assessed_land = _safe_float(d.get("landValue") or d.get("assessedLand"))
    assessed_improvement = _safe_float(
        d.get("improvementValue") or d.get("buildingValue") or d.get("assessedImprovement")
    )
    assessed_total = _safe_float(
        d.get("totalValue") or d.get("assessedTotal") or d.get("appraisedValue")
    )
    if assessed_total == 0.0 and (assessed_land or assessed_improvement):
        assessed_total = assessed_land + assessed_improvement

    tax_year = _safe_int(d.get("taxYear") or d.get("appraisalYear") or d.get("year"))
    building_sqft = _safe_int(
        d.get("buildingArea") or d.get("squareFeet") or d.get("livingArea") or d.get("bldgSqFt")
    )
    year_built = _safe_int(d.get("yearBuilt") or d.get("year_built"))
    use_description = (
        d.get("stateUseDescription") or d.get("useDescription") or
        d.get("propertyUse") or d.get("useCode") or ""
    ).strip()

    bedrooms_raw = d.get("bedrooms") or d.get("bedroomCount")
    bathrooms_raw = d.get("bathrooms") or d.get("bathroomCount") or d.get("fullBaths")
    bedrooms = _safe_int(bedrooms_raw) if bedrooms_raw is not None else None
    bathrooms = _safe_float(bathrooms_raw) if bathrooms_raw is not None else None

    out_of_state = mailing_state not in ("TX", "")
    absentee = bool(
        mailing_state and (
            out_of_state or
            (mailing_city and prop_city and
             mailing_city.upper() != prop_city.upper()[:len(mailing_city)])
        )
    )
    tax_delinquent = bool(d.get("taxDelinquent") or d.get("delinquent"))

    return {
        "owner_name": owner_name,
        "owner_mailing": owner_mailing,
        "property_address": prop_addr,
        "city": prop_city,
        "state": STATE,
        "zip": prop_zip,
        "county": COUNTY,
        "apn": apn,
        "assessed_land": assessed_land,
        "assessed_improvement": assessed_improvement,
        "assessed_total": assessed_total,
        "tax_year": tax_year,
        "building_sqft": building_sqft,
        "year_built": year_built,
        "use_description": use_description,
        "bedrooms": bedrooms,
        "bathrooms": bathrooms,
        "absentee_owner": absentee,
        "out_of_state_owner": out_of_state,
        "tax_delinquent": tax_delinquent,
        "source": SOURCE,
    }


def _error(msg):
    return {"error": msg, "source": SOURCE, "available": False}


def search_by_address(address):
    """
    Look up a parcel by situs address in Tarrant County (Fort Worth, TX).
    Returns PropIntel parcel dict or error dict.
    """
    if not address or not str(address).strip():
        return _error("No address provided")

    results, err = _search(str(address).strip())
    if err:
        return _error("TARCAD search failed: {}".format(err))
    if not results:
        return _error("No parcel found for address: {}".format(address))

    hit = results[0]
    prop_id = hit.get("prop_id") or hit.get("propertyId") or hit.get("id")
    if not prop_id:
        return _parse(hit)

    detail, err = _detail(prop_id)
    if err or not detail:
        return _parse(hit)

    return _parse(detail, search_hit=hit)


def search_by_apn(apn):
    """
    Look up a parcel by APN / account number in Tarrant County.
    """
    if not apn or not str(apn).strip():
        return _error("No APN provided")

    prop_id = str(apn).strip()
    detail, err = _detail(prop_id)
    if err or not detail:
        return _error("TARCAD APN lookup failed for {}: {}".format(apn, err or "no data"))

    return _parse(detail)
