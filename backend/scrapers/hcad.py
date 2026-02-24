"""
Harris Central Appraisal District (HCAD) — Property lookup via Harris County GIS ArcGIS REST API.
Uses Harris County's public ArcGIS service — no auth required, works from servers.
Source: https://www.gis.hctx.net/arcgis/rest/services/HCAD/Parcels/MapServer/0
"""
import urllib.request
import urllib.parse
import json
import re
import ssl
import logging

log = logging.getLogger(__name__)

BASE = "https://www.gis.hctx.net/arcgis/rest/services/HCAD/Parcels/MapServer/0"
SEARCH_URL = "https://hcad.org/property-search/"

FIELDS = ",".join([
    "HCAD_NUM", "acct_num", "tax_year",
    "owner_name_1", "owner_name_2",
    "mail_addr_1", "mail_addr_2", "mail_city", "mail_state", "mail_zip",
    "site_str_pfx", "site_str_num", "site_str_name", "site_str_sfx", "site_str_sfx_dir",
    "site_city", "site_county", "site_zip",
    "land_value", "bld_value", "impr_value", "total_appraised_val", "tax_value",
    "land_sqft", "land_use", "state_class", "dscr", "activeAccount_flag",
])

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Referer": "https://www.gis.hctx.net/",
}

# SSL context that tolerates self-signed certs on this county server
_SSL_CTX = ssl.create_default_context()
_SSL_CTX.check_hostname = False
_SSL_CTX.verify_mode = ssl.CERT_NONE


def _query(where_clause):
    """Query HCAD ArcGIS layer with a WHERE clause. Returns (features, error)."""
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
        log.warning("HCAD query error: %s", msg)
        return [], {"message": msg}


def _parse_address(address):
    """Split '5100 Westheimer Rd Houston TX' into (num, street_name)."""
    parts = address.upper().strip().split()
    if parts and parts[0].isdigit():
        num = parts[0]
        # Street name is everything after number until we hit a city/state word
        # Just use 2nd token as street name prefix
        street = parts[1] if len(parts) > 1 else ""
        return num, street
    return "", address.upper()[:20]


def _parse_feature(attrs, address):
    """Convert HCAD ArcGIS attributes to PropIntel parcel dict."""
    apn = (attrs.get("HCAD_NUM") or attrs.get("acct_num") or "").strip()
    owner = (attrs.get("owner_name_1") or "").strip()

    # Build property address
    pfx = (attrs.get("site_str_pfx") or "").strip()
    num = str(attrs.get("site_str_num") or "").strip()
    sname = (attrs.get("site_str_name") or "").strip()
    sfx = (attrs.get("site_str_sfx") or "").strip()
    sfx_dir = (attrs.get("site_str_sfx_dir") or "").strip()
    prop_parts = [p for p in [pfx, num, sname, sfx, sfx_dir] if p]
    prop_addr = " ".join(prop_parts)
    prop_city = (attrs.get("site_city") or "").strip()
    prop_zip = str(attrs.get("site_zip") or "").strip()

    # Build mailing address
    mail1 = (attrs.get("mail_addr_1") or "").strip()
    mail2 = (attrs.get("mail_addr_2") or "").strip()
    mail_city = (attrs.get("mail_city") or "").strip()
    mail_state = (attrs.get("mail_state") or "").strip()
    mail_zip = str(attrs.get("mail_zip") or "").strip()

    mailing_parts = [p for p in [mail1, mail2] if p]
    mailing = ", ".join(mailing_parts)
    if mail_city:
        mailing = (mailing + ", " + mail_city) if mailing else mail_city
    if mail_state:
        mailing = mailing + " " + mail_state
    if mail_zip:
        mailing = mailing + " " + mail_zip
    mailing = mailing.strip()

    # Financial values
    assessed_land = attrs.get("land_value")
    assessed_impr = attrs.get("impr_value") or attrs.get("bld_value")
    assessed_total = attrs.get("total_appraised_val") or attrs.get("tax_value")

    # Normalize numeric fields
    try:
        assessed_land = float(assessed_land) if assessed_land is not None else None
    except (TypeError, ValueError):
        assessed_land = None
    try:
        assessed_impr = float(assessed_impr) if assessed_impr is not None else None
    except (TypeError, ValueError):
        assessed_impr = None
    try:
        assessed_total = float(assessed_total) if assessed_total is not None else None
    except (TypeError, ValueError):
        assessed_total = None

    if assessed_land is not None and assessed_impr is None and assessed_total is not None:
        assessed_impr = assessed_total - assessed_land

    # Absentee / out-of-state flags
    owner_state = mail_state.upper() if mail_state else ""
    out_of_state = owner_state not in ("TX", "")
    # Absentee: mailing city differs from property city
    absentee = False
    if mail_city and prop_city:
        absentee = mail_city.upper() != prop_city.upper()
    elif mailing and prop_addr:
        absentee = prop_addr.upper() not in mailing.upper()

    tax_year_raw = attrs.get("tax_year")
    try:
        tax_year = int(tax_year_raw) if tax_year_raw else 0
    except (TypeError, ValueError):
        tax_year = 0

    land_sqft_raw = attrs.get("land_sqft")
    try:
        land_sqft = int(float(land_sqft_raw)) if land_sqft_raw else 0
    except (TypeError, ValueError):
        land_sqft = 0

    use_desc = (attrs.get("dscr") or attrs.get("state_class") or "").strip()

    return {
        "owner_name": owner,
        "owner_mailing": mailing or None,
        "property_address": prop_addr,
        "city": prop_city,
        "state": "TX",
        "zip": prop_zip,
        "county": "Harris",
        "apn": apn,
        "assessed_land": assessed_land,
        "assessed_improvement": assessed_impr,
        "assessed_total": assessed_total,
        "tax_year": tax_year,
        "building_sqft": land_sqft,
        "year_built": None,
        "use_description": use_desc,
        "bedrooms": None,
        "bathrooms": None,
        "absentee_owner": absentee,
        "out_of_state_owner": out_of_state,
        "tax_delinquent": False,
        "source": "hcad",
        # Extras for pipeline overlap
        "owner_city": mail_city,
        "owner_state": mail_state,
        "owner_zip": mail_zip,
        "hcad_direct_url": "https://hcad.org/property-search/property-detail/account/{}/".format(apn) if apn else SEARCH_URL,
    }


def _fallback(address, reason):
    encoded = urllib.parse.quote(str(address or ""))
    return {
        "error": reason,
        "source": "hcad",
        "manual_url": "{}?situs={}".format(SEARCH_URL, encoded),
    }


def search_by_address(address):
    """
    Search HCAD by situs address using Harris County GIS ArcGIS REST API.
    Returns parcel data dict or error dict.
    """
    if not address:
        return _fallback(address, "No address provided")

    num, street = _parse_address(address)
    street_safe = re.sub(r"['\"]", "", street)

    if num and street_safe:
        where = "site_str_num = '{}' AND site_str_name LIKE '{}%'".format(num, street_safe)
    elif num:
        where = "site_str_num = '{}'".format(num)
    else:
        where = "site_str_name LIKE '{}%'".format(street_safe[:20])

    features, err = _query(where)
    if features:
        return _parse_feature(features[0]["attributes"], address)

    # Fallback: broader search
    if num:
        features, err = _query("site_str_num = '{}'".format(num))
        if features:
            return _parse_feature(features[0]["attributes"], address)

    reason = (err or {}).get("message", "No parcel found") if err else "No parcel found for this address in HCAD"
    return _fallback(address, reason)
