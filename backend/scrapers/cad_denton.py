"""
Denton Central Appraisal District (DCAD) — Denton County, TX
Free property lookup via Denton County GIS ArcGIS REST API (public, no auth).

Source: https://gis.dentoncounty.gov/arcgis/rest/services/Parcels/MapServer/0
Portal: https://www.dentoncad.com/

Covers: Denton, Lewisville, Flower Mound, Carrollton (Denton portion), Frisco (Denton portion),
        The Colony, Little Elm, Highland Village, Corinth, Argyle, Krum, Sanger.
"""
import urllib.request
import urllib.parse
import json
import ssl
import logging

log = logging.getLogger(__name__)

BASE = "https://gis.dentoncounty.gov/arcgis/rest/services/Parcels/MapServer/0"
SEARCH_URL = "https://www.dentoncad.com/property-search"

FIELDS = ",".join([
    "OBJECTID", "prop_id", "OWNER_NAME",
    "SITUS", "CITY", "STATE",
    "ADDR_LINE1", "ADDR_LINE2", "ADDR_LINE3", "ZIP",
    "LIVINGAREA", "YR_BLT", "LAND_SQFT",
    "PROP_TYPE", "LEGAL_DESC", "ENTITIES",
    "STATE_CD", "EXEMPTION",
])

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Referer": "https://gis.dentoncounty.gov/",
}

_SSL_CTX = ssl.create_default_context()
_SSL_CTX.check_hostname = False
_SSL_CTX.verify_mode = ssl.CERT_NONE


def _query(where_clause):
    """Query Denton County GIS ArcGIS. Returns (features, error)."""
    try:
        params = urllib.parse.urlencode({
            "where": where_clause,
            "outFields": FIELDS,
            "returnGeometry": "false",
            "f": "json",
        })
        url = "{}/query?{}".format(BASE, params)
        req = urllib.request.Request(url, headers=HEADERS)
        with urllib.request.urlopen(req, timeout=12, context=_SSL_CTX) as resp:
            data = json.load(resp)
        if data.get("error"):
            return [], data["error"]
        return data.get("features", []), None
    except urllib.error.URLError as e:
        return [], {"message": "Denton GIS error: {}".format(e.reason)}
    except Exception as e:
        return [], {"message": str(e) or repr(e)}


def _parse(attrs, query):
    """Map Denton GIS attributes to PropIntel parcel dict."""
    def _clean_float(v):
        try:
            return float(v) if v is not None else None
        except (ValueError, TypeError):
            return None

    def _clean_int(v):
        try:
            return int(v) if v is not None else None
        except (ValueError, TypeError):
            return None

    owner = (attrs.get("OWNER_NAME") or "").strip()
    situs = (attrs.get("SITUS") or "").strip()
    prop_city = (attrs.get("CITY") or "").strip()

    # Mailing address
    addr1 = (attrs.get("ADDR_LINE1") or "").strip()
    addr2 = (attrs.get("ADDR_LINE2") or "").strip()
    addr3 = (attrs.get("ADDR_LINE3") or "").strip()
    owner_zip = attrs.get("ZIP")
    owner_zip_str = str(int(owner_zip)) if owner_zip else ""

    mailing_parts = [p for p in [addr1, addr2, addr3] if p]
    mailing = ", ".join(mailing_parts) if mailing_parts else None
    if mailing and owner_zip_str:
        mailing = "{} {}".format(mailing, owner_zip_str)

    living_sf = _clean_int(attrs.get("LIVINGAREA"))
    yr_built = _clean_int(attrs.get("YR_BLT"))
    land_sf = _clean_float(attrs.get("LAND_SQFT"))

    # Denton GIS doesn't expose assessed value — use None (AVM fallback handles it)
    absentee = bool(addr1 and prop_city and prop_city.upper() not in (addr1 + " " + addr2 + " " + addr3).upper())

    return {
        "source": "Denton Central Appraisal District",
        "source_url": SEARCH_URL,
        "apn": str(attrs.get("prop_id", "")).strip() or None,
        "owner_name": owner,
        "owner_mailing": mailing,
        "owner_city": "",
        "owner_state": "TX",
        "owner_zip": owner_zip_str,
        "property_address": situs,
        "property_city": prop_city,
        "legal_description": (attrs.get("LEGAL_DESC") or "").strip() or None,
        "use_code": (attrs.get("STATE_CD") or "").strip() or None,
        "use_description": (attrs.get("PROP_TYPE") or "").strip() or None,
        "building_sf": living_sf,
        "land_sf": int(land_sf) if land_sf else None,
        "year_built": yr_built if yr_built and yr_built > 1700 else None,
        "assessed_total": None,      # not in this layer
        "assessed_land": None,
        "assessed_improvement": None,
        "assessed_prev": None,
        "assessed_yoy_pct": None,
        "absentee_owner": absentee,
        "out_of_state_owner": False,
        "tax_delinquent": False,
        "denton_url": "https://www.dentoncad.com/property-search",
    }


def search_by_address(address):
    """Search Denton County GIS by SITUS address."""
    if not address:
        return _fallback(address, "No address provided")

    parts = address.upper().strip().split(",")[0].split()
    street_num = parts[0] if parts and parts[0].isdigit() else ""
    street_name = parts[1] if len(parts) > 1 and street_num else ""

    # Try progressively broader WHERE clauses
    candidates = []
    if street_num and street_name:
        candidates.append("SITUS LIKE '{} {}%'".format(street_num, street_name))
        # Handle directional prefix (e.g. "215 E MCKINNEY" stored as "215 E MCKINNEY ST")
        if len(parts) > 2 and len(parts[1]) <= 2:  # parts[1] might be direction
            candidates.append("SITUS LIKE '{} {} {}%'".format(street_num, parts[1], parts[2] if len(parts) > 2 else ""))
    if street_num:
        candidates.append("SITUS LIKE '{} %'".format(street_num))

    for where in candidates:
        features, err = _query(where)
        if err:
            log.warning("Denton GIS query error: %s", err)
            continue
        if features:
            # Best match: prefer exact street_name in SITUS
            best = features[0]
            if street_name:
                for f in features:
                    s = (f["attributes"].get("SITUS") or "").upper()
                    if street_name in s:
                        best = f
                        break
            return _parse(best["attributes"], address)

    return _fallback(address, "No parcel found for this address in Denton CAD")


def search_by_apn(apn):
    if not apn:
        return _fallback(apn, "No APN provided")
    features, err = _query("prop_id = {}".format(str(apn).strip()))
    if err or not features:
        return _fallback(apn, "APN not found in Denton CAD")
    return _parse(features[0]["attributes"], apn)


def _fallback(query, reason):
    encoded = urllib.parse.quote(str(query or ""))
    return {
        "source": "Denton Central Appraisal District",
        "source_url": SEARCH_URL,
        "warning": reason,
        "manual_url": "https://www.dentoncad.com/property-search",
        "note": "Visit DentonCAD.com to look up owner and value data.",
    }
