"""
Collin Central Appraisal District (CCAD) — Collin County, TX
Free property lookup via City of Allen ArcGIS REST API (public, no auth).
441,000+ parcels covering all of Collin County.

Source: https://gismaps.cityofallen.org/arcgis/rest/services/ReferenceData/
        Collin_County_Appraisal_District_Parcels/MapServer/1
Portal: https://esearch.collincad.org/

Covers: Frisco, McKinney, Allen, Plano (Collin portion), Richardson (Collin portion),
        Wylie, Murphy, Sachse (Collin portion), Garland (Collin portion),
        Princeton, Celina, Prosper, Anna, Melissa, Fairview, Lucas, Parker.
"""
import urllib.request
import urllib.parse
import json
import ssl
import logging

log = logging.getLogger(__name__)

BASE = (
    "https://gismaps.cityofallen.org/arcgis/rest/services"
    "/ReferenceData/Collin_County_Appraisal_District_Parcels/MapServer/1"
)
SEARCH_URL = "https://esearch.collincad.org/"

# Field prefix used by this endpoint
P = "GIS_DBO_AD_Entity_"

FIELDS = ",".join([
    "GIS_DBO_Parcel_PROP_ID",
    P + "file_as_name",
    P + "addr_line1", P + "addr_city", P + "addr_state", P + "addr_zip",
    P + "situs_num", P + "situs_street", P + "situs_city", P + "situs_zip",
    P + "curr_market", P + "cert_market", P + "curr_appraise", P + "cert_assessed",
    P + "living_area", P + "yr_blt", P + "beds", P + "baths", P + "land_sqft",
    P + "state_cd", P + "school",
])

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Referer": "https://gismaps.cityofallen.org/",
}

_SSL_CTX = ssl.create_default_context()
_SSL_CTX.check_hostname = False
_SSL_CTX.verify_mode = ssl.CERT_NONE


def _query(where_clause):
    """Query Collin CAD ArcGIS. Returns (features, error)."""
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
        with urllib.request.urlopen(req, timeout=15, context=_SSL_CTX) as resp:
            data = json.load(resp)
        if data.get("error"):
            return [], data["error"]
        return data.get("features", []), None
    except urllib.error.URLError as e:
        return [], {"message": "Collin CAD error: {}".format(e.reason)}
    except Exception as e:
        return [], {"message": str(e) or repr(e)}


def _f(attrs, key):
    """Get a prefixed field value, stripped."""
    val = attrs.get(P + key) or attrs.get(key)
    return str(val).strip() if val is not None else ""


def _num(attrs, key):
    """Get numeric field value."""
    val = attrs.get(P + key) or attrs.get(key)
    try:
        return float(val) if val is not None else None
    except (ValueError, TypeError):
        return None


def _parse(attrs, query):
    """Map Collin CAD ArcGIS attributes to PropIntel parcel dict."""
    owner = _f(attrs, "file_as_name")

    situs_city = _f(attrs, "situs_city")
    situs_zip = _f(attrs, "situs_zip")
    situs_num = _f(attrs, "situs_num")
    situs_street = _f(attrs, "situs_street")
    prop_addr = "{} {}".format(situs_num, situs_street).strip()

    owner_addr = _f(attrs, "addr_line1")
    owner_city = _f(attrs, "addr_city")
    owner_state = _f(attrs, "addr_state") or "TX"
    owner_zip = _f(attrs, "addr_zip").split("-")[0][:5]

    mailing = None
    if owner_addr:
        mailing = owner_addr
        if owner_city:
            mailing += ", {} {} {}".format(owner_city, owner_state, owner_zip)

    market = _num(attrs, "cert_market") or _num(attrs, "curr_market")
    appraised = _num(attrs, "curr_appraise")
    assessed = _num(attrs, "cert_assessed")
    market_val = market or appraised or assessed

    living_area = _num(attrs, "living_area")
    yr_blt_raw = _num(attrs, "yr_blt")
    yr_blt = int(yr_blt_raw) if yr_blt_raw and yr_blt_raw > 1700 else None
    beds = _num(attrs, "beds")
    baths = _num(attrs, "baths")
    land_sqft = _num(attrs, "land_sqft")

    absentee = bool(owner_city and situs_city and owner_city.upper() != situs_city.upper())
    out_of_state = bool(owner_state and owner_state.upper() not in ("TX", ""))

    apn = str(attrs.get("GIS_DBO_Parcel_PROP_ID", "")).strip()

    return {
        "source": "Collin Central Appraisal District",
        "source_url": SEARCH_URL,
        "apn": apn or None,
        "owner_name": owner,
        "owner_mailing": mailing,
        "owner_city": owner_city,
        "owner_state": owner_state,
        "owner_zip": owner_zip,
        "property_address": prop_addr or "{} {}".format(situs_num, situs_street).strip(),
        "property_city": situs_city,
        "property_zip": situs_zip,
        "use_code": _f(attrs, "state_cd") or None,
        "school_district": _f(attrs, "school") or None,
        "building_sf": int(living_area) if living_area else None,
        "land_sf": int(land_sqft) if land_sqft else None,
        "year_built": yr_blt,
        "bedrooms": int(beds) if beds else None,
        "bathrooms": int(baths) if baths else None,
        "assessed_total": market_val,
        "assessed_land": None,
        "assessed_improvement": None,
        "assessed_prev": None,
        "assessed_yoy_pct": None,
        "absentee_owner": absentee,
        "out_of_state_owner": out_of_state,
        "tax_delinquent": False,
        "ccad_url": "https://esearch.collincad.org/" + ("?prop_id={}".format(apn) if apn else ""),
    }


def search_by_address(address):
    """Search Collin CAD by situs address."""
    if not address:
        return _fallback(address, "No address provided")

    street_part = address.upper().strip().split(",")[0].strip()
    parts = street_part.split()
    street_num = parts[0] if parts and parts[0].isdigit() else ""
    street_name = parts[1] if len(parts) > 1 and street_num else ""

    candidates = []
    if street_num and street_name:
        # Exact number + start of street name
        candidates.append(
            "{p}situs_num = '{n}' AND {p}situs_street LIKE '{s}%'".format(
                p=P, n=street_num, s=street_name
            )
        )
        # Handle directional prefix (E, W, N, S)
        if len(street_name) <= 2 and len(parts) > 2:
            candidates.append(
                "{p}situs_num = '{n}' AND {p}situs_street LIKE '{s}%'".format(
                    p=P, n=street_num, s=parts[2]
                )
            )
    if street_num:
        candidates.append("{p}situs_num = '{n}'".format(p=P, n=street_num))

    for where in candidates:
        features, err = _query(where)
        if err:
            log.warning("Collin CAD query error: %s", err)
            continue
        if features:
            # Pick best match: prefer street_name in situs_street
            best = features[0]
            if street_name:
                for f in features:
                    st = _f(f["attributes"], "situs_street")
                    st1 = _f(f["attributes"], "situs_street1")
                    if street_name in st.upper() or street_name in st1.upper():
                        best = f
                        break
            return _parse(best["attributes"], address)

    return _fallback(address, "No parcel found in Collin CAD")


def search_by_apn(apn):
    if not apn:
        return _fallback(apn, "No APN provided")
    features, err = _query("GIS_DBO_Parcel_PROP_ID = '{}'".format(str(apn).strip()))
    if err or not features:
        return _fallback(apn, "APN not found in Collin CAD")
    return _parse(features[0]["attributes"], apn)


def _fallback(query, reason):
    return {
        "source": "Collin Central Appraisal District",
        "source_url": SEARCH_URL,
        "warning": reason,
        "manual_url": SEARCH_URL,
        "note": "Visit esearch.collincad.org to look up owner and value data.",
    }
