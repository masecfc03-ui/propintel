"""
Dallas Central Appraisal District (DCAD) — Property lookup via ArcGIS REST API.
Uses DCAD's internal map portal ArcGIS service — public, no auth required, works from servers.
Source: https://maps.dcad.org/prdwa/rest/services/Property/ParcelHistory/MapServer
"""
import requests
import re

BASE = "https://maps.dcad.org/prdwa/rest/services/Property/ParcelHistory/MapServer"
SEARCH_URL = "https://www.dcad.org/property-search/"

# Layer IDs by year: 0=2021, 1=2022, 2=2023, 3=2024, 4=2025
CURRENT_LAYER = 4   # Most recent
FALLBACK_LAYER = 3  # One year back

FIELDS = ",".join([
    "PARCELID", "LOWPARCELID", "OWNERNME1", "OWNERNME2",
    "SITEADDRESS", "PRPRTYDSCRP", "CNVYNAME",
    "PSTLADDRESS", "PSTLCITY", "PSTLSTATE", "PSTLZIP5",
    "USECD", "USEDSCRP", "CLASSDSCRP",
    "CNTASSDVAL", "LNDVALUE", "PRVASSDVAL",
    "BLDGAREA", "RESYRBLT",
    "CVTTXDSCRP", "SCHLDSCRP", "MAPGRID", "REVALYR"
])

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Referer": "https://maps.dcad.org/",
}


def _query_layer(where_clause, layer=CURRENT_LAYER):
    """Query a DCAD ArcGIS layer with a WHERE clause."""
    try:
        resp = requests.get(
            f"{BASE}/{layer}/query",
            params={
                "where": where_clause,
                "outFields": FIELDS,
                "returnGeometry": "false",
                "f": "json",
            },
            headers=HEADERS,
            timeout=12,  # Fail fast — fallback if blocked from Render IPs
        )
        resp.raise_for_status()
        data = resp.json()
        # ArcGIS returns {"error": {"code":...}} on failure
        if data.get("error"):
            return [], data["error"]
        return data.get("features", []), None
    except requests.exceptions.Timeout:
        return [], {"message": "DCAD ArcGIS timeout — server may be blocking this IP range"}
    except requests.exceptions.ConnectionError as e:
        return [], {"message": f"DCAD connection error: {str(e) or 'Connection refused'}"}
    except Exception as e:
        return [], {"message": str(e) or repr(e)}


def _parse_feature(attrs, query):
    """Convert ArcGIS attributes dict to PropIntel parcel data."""
    apn = attrs.get("PARCELID") or attrs.get("LOWPARCELID")
    owner = (attrs.get("OWNERNME1") or "").strip()
    owner2 = (attrs.get("OWNERNME2") or "").strip()
    
    postal_addr = (attrs.get("PSTLADDRESS") or "").strip()
    postal_city = (attrs.get("PSTLCITY") or "").strip()
    postal_state = (attrs.get("PSTLSTATE") or "").strip()
    postal_zip = (attrs.get("PSTLZIP5") or "").strip()
    
    mailing = None
    if postal_addr:
        mailing = postal_addr
        if postal_city:
            mailing += ", " + postal_city
        if postal_state:
            mailing += " " + postal_state
        if postal_zip:
            mailing += " " + postal_zip
    
    assessed_total = attrs.get("CNTASSDVAL")
    assessed_land = attrs.get("LNDVALUE")
    assessed_prev = attrs.get("PRVASSDVAL")
    
    # Detect absentee/out-of-state owner
    property_city = "GARLAND"
    owner_state = postal_state.upper() if postal_state else ""
    absentee = bool(mailing and "GARLAND" not in postal_city.upper()) if postal_city else False
    out_of_state = (owner_state not in ("TX", "")) if owner_state else False
    
    # Year-over-year value change
    yoy_change = None
    if assessed_total and assessed_prev and assessed_prev > 0:
        yoy_change = round(((assessed_total - assessed_prev) / assessed_prev) * 100, 1)
    
    result = {
        "source": "Dallas Central Appraisal District",
        "source_url": SEARCH_URL,
        "apn": apn,
        "owner_name": owner,
        "owner_name2": owner2 or None,
        "owner_mailing": mailing,
        "owner_city": postal_city,
        "owner_state": postal_state,
        "owner_zip": postal_zip,
        "property_address": (attrs.get("SITEADDRESS") or "").strip(),
        "legal_description": (attrs.get("PRPRTYDSCRP") or "").strip(),
        "subdivision": (attrs.get("CNVYNAME") or "").strip() or None,
        "use_code": attrs.get("USECD"),
        "use_description": (attrs.get("USEDSCRP") or "").strip(),
        "property_class": (attrs.get("CLASSDSCRP") or "").strip(),
        "tax_district": (attrs.get("CVTTXDSCRP") or "").strip(),
        "school_district": (attrs.get("SCHLDSCRP") or "").strip(),
        "building_sf": attrs.get("BLDGAREA"),
        "year_built": attrs.get("RESYRBLT"),
        "assessed_total": assessed_total,
        "assessed_land": assessed_land,
        "assessed_improvement": (assessed_total - assessed_land) if (assessed_total and assessed_land) else None,
        "assessed_prev": assessed_prev,
        "assessed_yoy_pct": yoy_change,
        "revalue_year": attrs.get("REVALYR"),
        "absentee_owner": absentee,
        "out_of_state_owner": out_of_state,
        "tax_delinquent": False,  # Not available in this API — flag for manual check
        "dcad_direct_url": f"https://www.dcad.org/property-search/?account={apn}" if apn else SEARCH_URL,
    }
    return result


def search_by_address(address):
    """
    Search DCAD by situs (property) address using ArcGIS REST API.
    Returns parcel data dict or fallback with manual link.
    """
    if not address:
        return _dcad_fallback(address, "No address provided")

    # Parse street number and street name for fuzzy match
    parts = address.upper().strip().split()
    street_num = parts[0] if parts and parts[0].isdigit() else ""
    street_name = parts[1] if len(parts) > 1 else ""

    where = ""
    if street_num and street_name:
        where = f"SITEADDRESS LIKE '{street_num} {street_name}%'"
    elif street_num:
        where = f"SITEADDRESS LIKE '{street_num}%'"
    else:
        where = f"SITEADDRESS LIKE '%{address.upper()[:20]}%'"

    # Try current year first, fall back to prior year
    for layer in [CURRENT_LAYER, FALLBACK_LAYER]:
        features, err = _query_layer(where, layer)
        if features:
            return _parse_feature(features[0]["attributes"], address)

    # No result
    return _dcad_fallback(address, "No parcel found for this address in DCAD")


def search_by_apn(apn):
    """Search DCAD by Account/Parcel ID."""
    if not apn:
        return _dcad_fallback(apn, "No APN provided")

    clean_apn = re.sub(r"[^0-9]", "", str(apn))
    where = f"PARCELID = '{clean_apn}'"

    for layer in [CURRENT_LAYER, FALLBACK_LAYER]:
        features, err = _query_layer(where, layer)
        if features:
            return _parse_feature(features[0]["attributes"], apn)

    return _dcad_fallback(apn, "APN not found in DCAD")


def _dcad_fallback(query, reason):
    """Structured fallback when DCAD lookup fails."""
    encoded = requests.utils.quote(str(query or ""))
    return {
        "source": "Dallas Central Appraisal District",
        "source_url": SEARCH_URL,
        "warning": reason,
        "manual_url": f"https://www.dcad.org/property-search/?situs={encoded}",
        "note": "Direct DCAD lookup required. Use the link to retrieve owner, APN, assessed value, and tax status.",
    }
