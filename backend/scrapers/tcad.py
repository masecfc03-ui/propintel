"""
Travis Central Appraisal District (TCAD) — Property lookup via Travis County GIS ArcGIS REST API.
Uses Travis County's public ArcGIS service — no auth required, works from servers.
Source: https://gis.traviscountytx.gov/server1/rest/services/Boundaries_and_Jurisdictions/
        TCAD_Travis_County_Property/MapServer/3
"""
import urllib.request
import urllib.parse
import json
import re
import ssl
import logging

log = logging.getLogger(__name__)

BASE = (
    "https://gis.traviscountytx.gov/server1/rest/services"
    "/Boundaries_and_Jurisdictions/TCAD_Travis_County_Property/MapServer/3"
)
SEARCH_URL = "https://traviscad.org/propertysearch/"

FIELDS = ",".join([
    "PROP_ID", "geo_id", "py_owner_name", "py_owner_id",
    "situs_address", "situs_num", "situs_street", "situs_street_prefx",
    "situs_street_suffix", "situs_city", "situs_zip",
    "py_address",
    "appraised_val", "assessed_val", "market_value",
    "land_homesite_val", "land_non_homesite_val",
    "imprv_homesite_val", "imprv_non_homesite_val",
    "land_state_cd", "land_type_desc",
    "legal_desc", "sub_dec", "entities",
    "deed_date", "hyperlink",
])

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Referer": "https://gis.traviscountytx.gov/",
}

_SSL_CTX = ssl.create_default_context()
_SSL_CTX.check_hostname = False
_SSL_CTX.verify_mode = ssl.CERT_NONE


def _query(where_clause):
    """Query TCAD ArcGIS layer. Returns (features, error)."""
    try:
        params = urllib.parse.urlencode({
            "where": where_clause,
            "outFields": FIELDS,
            "returnGeometry": "false",
            "resultRecordCount": "5",
            "f": "json",
        })
        url = "{}/query?{}".format(BASE, params)
        req = urllib.request.Request(url, headers=HEADERS)
        with urllib.request.urlopen(req, timeout=12, context=_SSL_CTX) as resp:
            data = json.load(resp)
        if data.get("error"):
            return [], data["error"]
        return data.get("features", []), None
    except Exception as exc:
        msg = str(exc) or repr(exc)
        log.warning("TCAD query error: %s", msg)
        return [], {"message": msg}


def _parse_owner_address(py_address):
    """
    Parse py_address like 'TRAVIS COUNTY CT HOUSE PO BOX 1748 AUSTIN TX 78767'
    into (addr_str, city, state, zip). Very loose — best-effort parsing.
    """
    if not py_address:
        return None, None, None, None
    raw = py_address.strip()
    # Try to detect zip at end
    zip_code = None
    state = None
    city = None
    m = re.search(r"\b([A-Z]{2})\s+(\d{5})(-\d{4})?\s*$", raw)
    if m:
        state = m.group(1)
        zip_code = m.group(2)
        raw = raw[:m.start()].strip()
        # The last word(s) before state are the city
        words = raw.split()
        if len(words) >= 2:
            city = words[-1]
            addr = " ".join(words[:-1])
        else:
            addr = raw
            city = None
    else:
        addr = raw
    return addr or None, city, state, zip_code


def _parse_feature(attrs, address):
    """Convert TCAD ArcGIS attributes to PropIntel parcel dict."""
    apn = str(attrs.get("PROP_ID") or attrs.get("geo_id") or "").strip()
    owner = (attrs.get("py_owner_name") or "").strip()

    # Owner mailing address
    py_addr_raw = (attrs.get("py_address") or "").strip()
    mail_addr, mail_city, mail_state, mail_zip = _parse_owner_address(py_addr_raw)
    mailing = py_addr_raw  # Use raw string as mailing address; it's already formatted

    # Property address
    prop_addr = (attrs.get("situs_address") or "").strip()
    # Extract city (often not in situs_address for TCAD — use situs_city)
    prop_city = (attrs.get("situs_city") or "").strip()
    if not prop_city:
        prop_city = "Austin"
    prop_zip = str(attrs.get("situs_zip") or "").strip()

    # Financial values
    def _to_float(v):
        if v is None:
            return None
        try:
            return float(v)
        except (TypeError, ValueError):
            return None

    land_homesite = _to_float(attrs.get("land_homesite_val")) or 0.0
    land_non_hs = _to_float(attrs.get("land_non_homesite_val")) or 0.0
    assessed_land = land_homesite + land_non_hs
    if assessed_land == 0.0:
        assessed_land = None

    impr_homesite = _to_float(attrs.get("imprv_homesite_val")) or 0.0
    impr_non_hs = _to_float(attrs.get("imprv_non_homesite_val")) or 0.0
    assessed_impr = impr_homesite + impr_non_hs
    if assessed_impr == 0.0:
        assessed_impr = None

    assessed_total = _to_float(attrs.get("appraised_val")) or _to_float(attrs.get("assessed_val"))
    if assessed_total is None and assessed_land is not None and assessed_impr is not None:
        assessed_total = assessed_land + assessed_impr

    owner_state_str = mail_state.upper() if mail_state else ""
    out_of_state = owner_state_str not in ("TX", "")
    absentee = False
    if mail_city and prop_city:
        absentee = mail_city.upper() != prop_city.upper()

    use_desc = (attrs.get("land_type_desc") or attrs.get("land_state_cd") or "").strip()

    return {
        "owner_name": owner,
        "owner_mailing": mailing or None,
        "property_address": prop_addr,
        "city": prop_city,
        "state": "TX",
        "zip": prop_zip,
        "county": "Travis",
        "apn": apn,
        "assessed_land": assessed_land,
        "assessed_improvement": assessed_impr,
        "assessed_total": assessed_total,
        "tax_year": 0,  # Not in this layer
        "building_sqft": 0,
        "year_built": None,
        "use_description": use_desc,
        "bedrooms": None,
        "bathrooms": None,
        "absentee_owner": absentee,
        "out_of_state_owner": out_of_state,
        "tax_delinquent": False,
        "source": "tcad",
        # Extras
        "owner_city": mail_city,
        "owner_state": mail_state,
        "owner_zip": mail_zip,
        "legal_description": (attrs.get("legal_desc") or "").strip(),
        "tcad_direct_url": attrs.get("hyperlink") or "https://traviscad.org/propertysearch/",
    }


def _fallback(address, reason):
    encoded = urllib.parse.quote(str(address or ""))
    return {
        "error": reason,
        "source": "tcad",
        "manual_url": "{}?search={}".format(SEARCH_URL, encoded),
    }


def search_by_address(address):
    """
    Search TCAD by situs address using Travis County GIS ArcGIS REST API.
    Returns parcel data dict or error dict.

    TCAD situs format: '1005 NUECES ST 78701' (number + street + suffix + zip)
    """
    if not address:
        return _fallback(address, "No address provided")

    addr_upper = address.upper().strip()
    addr_safe = re.sub(r"['\"]", "", addr_upper)

    parts = addr_safe.split()
    num = parts[0] if parts and parts[0].isdigit() else ""
    street = parts[1] if len(parts) > 1 else ""

    if num and street:
        where = "situs_address LIKE '{} {}%'".format(num, street)
    elif num:
        where = "situs_address LIKE '{}%'".format(num)
    else:
        where = "situs_address LIKE '{}%'".format(addr_safe[:25])

    features, err = _query(where)
    if features:
        return _parse_feature(features[0]["attributes"], address)

    reason = (err or {}).get("message", "No parcel found") if err else "No parcel found for this address in TCAD"
    return _fallback(address, reason)
