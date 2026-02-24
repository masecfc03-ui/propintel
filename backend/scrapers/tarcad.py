"""
Tarrant County Appraisal District (TARCAD) — Fort Worth, TX
Free property lookup via TAD ArcGIS REST API (public, no auth).

Source: https://tad.newedgeservices.com/arcgis/rest/services/OD_TAD/OD_ParcelView/MapServer/0
Portal: https://www.tad.org/

Covers: Fort Worth, Arlington, Mansfield, Euless, Bedford, Hurst, North Richland Hills,
        Grapevine, Southlake, Colleyville, Keller, and all Tarrant County cities.
"""
import urllib.request
import urllib.parse
import json
import ssl
import re
import logging

log = logging.getLogger(__name__)

BASE = (
    "https://tad.newedgeservices.com/arcgis/rest/services"
    "/OD_TAD/OD_ParcelView/MapServer/0"
)
SEARCH_URL = "https://www.tad.org/property-search"

FIELDS = ",".join([
    "TAXPIN", "Account_Nu", "Owner_Name", "Owner_Addr",
    "Owner_City", "Owner_Zip",
    "Situs_Addr", "City", "ZipCode",
    "Total_Valu", "Land_Value", "Improvemen", "Appraised_",
    "Year_Built", "Living_Are", "Num_Bedroo", "Num_Bathro",
    "State_Use_", "LegalDescr", "School", "Deed_Date",
    "Ag_Code", "Land_SqFt", "Land_Acres",
])

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Referer": "https://tad.newedgeservices.com/",
}

_SSL_CTX = ssl.create_default_context()
_SSL_CTX.check_hostname = False
_SSL_CTX.verify_mode = ssl.CERT_NONE


def _query(where_clause):
    """Query TARCAD ArcGIS MapServer. Returns (features, error)."""
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
        return [], {"message": "TARCAD connection error: {}".format(e.reason)}
    except Exception as e:
        return [], {"message": str(e) or repr(e)}


def _parse(attrs, query):
    """Map TARCAD ArcGIS attributes to PropIntel parcel dict."""
    def _clean_int(v):
        try:
            return int(str(v).strip()) if v else None
        except (ValueError, TypeError):
            return None

    def _clean_float(v):
        try:
            return float(str(v).strip()) if v else None
        except (ValueError, TypeError):
            return None

    owner = (attrs.get("Owner_Name") or "").strip()
    owner_addr = (attrs.get("Owner_Addr") or "").strip()
    owner_city = (attrs.get("Owner_City") or "").strip()
    owner_zip = (attrs.get("Owner_Zip") or "").strip()
    prop_city = (attrs.get("City") or "").strip()
    prop_zip = (attrs.get("ZipCode") or "").strip()
    situs = (attrs.get("Situs_Addr") or "").strip()

    mailing = owner_addr or None
    if mailing and owner_city:
        mailing = "{}, {} TX {}".format(owner_addr, owner_city, owner_zip).strip(", ")

    total_val = _clean_float(attrs.get("Total_Valu"))
    land_val = _clean_float(attrs.get("Land_Value"))
    imprv_val = _clean_float(attrs.get("Improvemen"))
    appraised = _clean_float(attrs.get("Appraised_"))

    # Use appraised or total, whichever is available and > 0
    market_value = appraised if (appraised and appraised > 0) else (total_val if (total_val and total_val > 0) else None)

    yr_built = _clean_int(attrs.get("Year_Built"))
    living_sf = _clean_int(attrs.get("Living_Are"))
    bedrooms = _clean_int(attrs.get("Num_Bedroo"))
    bathrooms = _clean_int(attrs.get("Num_Bathro"))
    land_sf = _clean_int(attrs.get("Land_SqFt"))

    # Absentee / out-of-state detection
    owner_state_raw = ""
    if owner_city and owner_zip and "TX" not in owner_addr.upper():
        # Heuristic: if mailing city differs from property city, absentee
        absentee = owner_city.upper() != prop_city.upper() if prop_city else False
    else:
        absentee = False

    out_of_state = False  # TAD doesn't expose owner state directly

    apn = (attrs.get("TAXPIN") or attrs.get("Account_Nu") or "").strip()

    return {
        "source": "Tarrant County Appraisal District",
        "source_url": SEARCH_URL,
        "apn": apn,
        "owner_name": owner,
        "owner_mailing": mailing,
        "owner_city": owner_city,
        "owner_state": "TX",
        "owner_zip": owner_zip,
        "property_address": situs,
        "property_city": prop_city,
        "property_zip": prop_zip or prop_zip,
        "legal_description": (attrs.get("LegalDescr") or "").strip() or None,
        "use_code": (attrs.get("State_Use_") or "").strip() or None,
        "school_district": (attrs.get("School") or "").strip() or None,
        "assessed_total": market_value,
        "assessed_land": land_val if land_val and land_val > 0 else None,
        "assessed_improvement": imprv_val if imprv_val and imprv_val > 0 else None,
        "assessed_prev": None,
        "assessed_yoy_pct": None,
        "building_sf": living_sf,
        "land_sf": land_sf,
        "year_built": yr_built if yr_built and yr_built > 1700 else None,
        "bedrooms": bedrooms,
        "bathrooms": bathrooms,
        "deed_date": (attrs.get("Deed_Date") or "").strip() or None,
        "absentee_owner": absentee,
        "out_of_state_owner": out_of_state,
        "tax_delinquent": False,
        "tad_url": "https://www.tad.org/property-search?account={}".format(apn) if apn else SEARCH_URL,
    }


def search_by_address(address):
    """
    Search TARCAD ArcGIS by situs address.
    Tries: '1234 MAIN%' then '1234%' fallback.
    """
    if not address:
        return _fallback(address, "No address provided")

    parts = address.upper().strip().split()
    street_num = parts[0] if parts and parts[0].isdigit() else ""
    street_name = parts[1] if len(parts) > 1 and street_num else ""

    # Build progressively broader WHERE clauses
    candidates = []
    if street_num and street_name:
        candidates.append("Situs_Addr LIKE '{} {}%'".format(street_num, street_name))
    if street_num:
        candidates.append("Situs_Addr LIKE '{}%'".format(street_num))

    for where in candidates:
        features, err = _query(where)
        if err:
            log.warning("TARCAD query error: %s", err)
            continue
        if features:
            # Pick best match if multiple (prefer exact street number match)
            best = features[0]
            for f in features:
                situs = (f["attributes"].get("Situs_Addr") or "").upper()
                if street_num and situs.startswith(street_num + " "):
                    if street_name and street_name in situs:
                        best = f
                        break
                    elif not street_name:
                        best = f
                        break
            return _parse(best["attributes"], address)

    return _fallback(address, "No parcel found for this address in Tarrant CAD")


def search_by_apn(apn):
    """Search TARCAD by TAXPIN or Account Number."""
    if not apn:
        return _fallback(apn, "No APN provided")
    clean = str(apn).strip()
    features, err = _query("TAXPIN = '{}' OR Account_Nu = '{}'".format(clean, clean))
    if err or not features:
        return _fallback(apn, "APN not found in Tarrant CAD")
    return _parse(features[0]["attributes"], apn)


def _fallback(query, reason):
    encoded = urllib.parse.quote(str(query or ""))
    return {
        "source": "Tarrant County Appraisal District",
        "source_url": SEARCH_URL,
        "warning": reason,
        "manual_url": "https://www.tad.org/property-search?search={}".format(encoded),
        "note": "Visit TAD.org to manually retrieve owner and value data.",
    }
