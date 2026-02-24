"""
Bexar Central Appraisal District (BCAD) — Property lookup via BCAD ArcGIS REST API.
Uses BCAD's public map portal ArcGIS service — no auth required, works from servers.
Source: https://maps.bcad.org/arcgis/rest/services/PAMapSearch/MapServer/6
Note: Appraised values not exposed in this layer; ownership and address data are available.
"""
import urllib.request
import urllib.parse
import json
import re
import ssl
import logging

log = logging.getLogger(__name__)

BASE = "https://maps.bcad.org/arcgis/rest/services/PAMapSearch/MapServer/6"
SEARCH_URL = "https://www.bcad.org/"

# Fully-qualified field names as returned by the service
F_PROP_ID = "PAMaps.DBO.ParcelFabric_Parcels.PROP_ID"
F_OWNER_NAME = "PAMaps.dbo.web_map_property.owner_name"
F_ADDR1 = "PAMaps.dbo.web_map_property.addr_line1"
F_ADDR2 = "PAMaps.dbo.web_map_property.addr_line2"
F_ADDR3 = "PAMaps.dbo.web_map_property.addr_line3"
F_CITY = "PAMaps.dbo.web_map_property.addr_city"
F_STATE = "PAMaps.dbo.web_map_property.addr_state"
F_ZIP = "PAMaps.dbo.web_map_property.addr_zip"
F_SITUS = "PAMaps.dbo.web_map_property.situs"
F_YEAR = "PAMaps.dbo.web_map_property.prop_val_yr"
F_TYPE = "PAMaps.dbo.web_map_property.prop_type_desc"
F_STATE_CD = "PAMaps.dbo.web_map_property.state_cd"
F_LEGAL = "PAMaps.dbo.web_map_property.legal_desc"
F_APPRAISED = "PAMaps.dbo.web_map_property.appraised_val"

FIELDS = ",".join([
    F_PROP_ID, F_OWNER_NAME,
    F_ADDR1, F_ADDR2, F_ADDR3, F_CITY, F_STATE, F_ZIP,
    F_SITUS, F_YEAR, F_TYPE, F_STATE_CD, F_LEGAL, F_APPRAISED,
])

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Referer": "https://maps.bcad.org/",
}

_SSL_CTX = ssl.create_default_context()
_SSL_CTX.check_hostname = False
_SSL_CTX.verify_mode = ssl.CERT_NONE


def _query(where_clause):
    """Query BCAD ArcGIS layer. Returns (features, error)."""
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
        log.warning("BCAD query error: %s", msg)
        return [], {"message": msg}


def _parse_situs(situs):
    """Parse BCAD situs string 'NUM STREET CITY, TX ZIP' into parts."""
    # Format: "100 GOLIAD RD  SAN ANTONIO, TX 78223"
    if not situs:
        return "", "", "", "TX", ""
    situs = situs.strip()
    # Try to extract zip at end
    zip_code = ""
    m = re.search(r"\s(\d{5})(-\d{4})?$", situs)
    if m:
        zip_code = m.group(1)
        situs = situs[:m.start()].strip()
    # Remove state
    m2 = re.search(r",\s*TX\s*$", situs, re.IGNORECASE)
    city = ""
    if m2:
        situs = situs[:m2.start()].strip()
        # Everything after last comma-like boundary is city
        # Format at this point: "NUM STREET CITY"
        # Try to find the city (which is the part after the street address)
        # This is tricky since we don't know where street ends and city begins
        # Use a simplification: if there's a comma, city is after it
        if "," in situs:
            parts = situs.rsplit(",", 1)
            street_part = parts[0].strip()
            city = parts[1].strip()
        else:
            street_part = situs
            city = "SAN ANTONIO"
    else:
        street_part = situs
        city = ""
    prop_addr = street_part
    return prop_addr, city, "TX", zip_code


def _parse_feature(attrs, address):
    """Convert BCAD ArcGIS attributes to PropIntel parcel dict."""
    apn = str(attrs.get(F_PROP_ID) or "").strip()
    owner = (attrs.get(F_OWNER_NAME) or "").strip()

    # Mailing address
    addr1 = (attrs.get(F_ADDR1) or "").strip()
    addr2 = (attrs.get(F_ADDR2) or "").strip()
    addr3 = (attrs.get(F_ADDR3) or "").strip()
    mail_city = (attrs.get(F_CITY) or "").strip()
    mail_state = (attrs.get(F_STATE) or "").strip()
    mail_zip = (attrs.get(F_ZIP) or "").strip()

    mail_lines = [p for p in [addr1, addr2, addr3] if p]
    mailing = " ".join(mail_lines)
    if mail_city:
        mailing = (mailing + ", " + mail_city) if mailing else mail_city
    if mail_state:
        mailing = mailing + " " + mail_state
    if mail_zip:
        mailing = mailing + " " + mail_zip
    mailing = mailing.strip()

    # Property address from situs
    situs_raw = (attrs.get(F_SITUS) or "").strip()
    prop_addr, prop_city, _, prop_zip = _parse_situs(situs_raw)
    # If situs city not parsed, use San Antonio as default for BCAD
    if not prop_city:
        prop_city = "San Antonio"

    # Financial values - this layer returns "N/A" string for most properties
    appraised_raw = attrs.get(F_APPRAISED)
    assessed_total = None
    if appraised_raw is not None and appraised_raw != "N/A":
        try:
            assessed_total = float(str(appraised_raw).replace(",", ""))
        except (TypeError, ValueError):
            assessed_total = None

    tax_year_raw = attrs.get(F_YEAR)
    try:
        tax_year = int(tax_year_raw) if tax_year_raw else 0
    except (TypeError, ValueError):
        tax_year = 0

    use_desc = (attrs.get(F_TYPE) or attrs.get(F_STATE_CD) or "").strip()

    owner_state = mail_state.upper() if mail_state else ""
    out_of_state = owner_state not in ("TX", "")
    absentee = False
    if mail_city and prop_city:
        absentee = mail_city.upper() != prop_city.upper()

    return {
        "owner_name": owner,
        "owner_mailing": mailing or None,
        "property_address": prop_addr,
        "city": prop_city,
        "state": "TX",
        "zip": prop_zip,
        "county": "Bexar",
        "apn": apn,
        "assessed_land": None,
        "assessed_improvement": None,
        "assessed_total": assessed_total,
        "tax_year": tax_year,
        "building_sqft": 0,
        "year_built": None,
        "use_description": use_desc,
        "bedrooms": None,
        "bathrooms": None,
        "absentee_owner": absentee,
        "out_of_state_owner": out_of_state,
        "tax_delinquent": False,
        "source": "bcad",
        # Extras
        "owner_city": mail_city,
        "owner_state": mail_state,
        "owner_zip": mail_zip,
        "legal_description": (attrs.get(F_LEGAL) or "").strip(),
        "bcad_direct_url": "https://www.bcad.org/search?pid={}".format(apn) if apn else SEARCH_URL,
    }


def _fallback(address, reason):
    encoded = urllib.parse.quote(str(address or ""))
    return {
        "error": reason,
        "source": "bcad",
        "manual_url": "https://www.bcad.org/search?address={}".format(encoded),
    }


def search_by_address(address):
    """
    Search BCAD by situs address using BCAD ArcGIS PAMapSearch REST API.
    Returns parcel data dict or error dict.
    """
    if not address:
        return _fallback(address, "No address provided")

    addr_upper = address.upper().strip()
    # Clean for SQL safety
    addr_safe = re.sub(r"['\"]", "", addr_upper)

    # Parse street number and first word of street name
    parts = addr_safe.split()
    num = parts[0] if parts and parts[0].isdigit() else ""
    street = parts[1] if len(parts) > 1 else ""

    # BCAD situs format: "NUM STREET CITY, TX ZIP"
    # Search using the full field name path
    if num and street:
        where = "PAMaps.dbo.web_map_property.situs LIKE '{} {}%'".format(num, street)
    elif num:
        where = "PAMaps.dbo.web_map_property.situs LIKE '{}%'".format(num)
    else:
        where = "PAMaps.dbo.web_map_property.situs LIKE '{}%'".format(addr_safe[:25])

    features, err = _query(where)
    if features:
        return _parse_feature(features[0]["attributes"], address)

    reason = (err or {}).get("message", "No parcel found") if err else "No parcel found for this address in BCAD"
    return _fallback(address, reason)
