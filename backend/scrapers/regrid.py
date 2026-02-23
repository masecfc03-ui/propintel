"""
Regrid — National Parcel Data API
156M+ parcel records across all 3,200 US counties.

Trial token: 7 counties, expires 2026-03-25
Paid plan ($150/mo Self-Serve): Full national coverage

API docs: https://support.regrid.com/api
Base URL: https://app.regrid.com/api/v2
"""
import os
import requests

REGRID_TOKEN = os.getenv("REGRID_API_KEY", "")
BASE_URL = "https://app.regrid.com/api/v2"

HEADERS = {
    "User-Agent": "PropIntel/1.0 (propertyvalueintel.com)",
    "Accept": "application/json",
}


def _params(**kwargs):
    """Build params dict with token injected."""
    p = {"token": REGRID_TOKEN}
    p.update({k: v for k, v in kwargs.items() if v is not None})
    return p


def _parse_parcel(feature):
    """Convert a Regrid GeoJSON feature into PropIntel parcel dict."""
    if not feature:
        return None

    props = feature.get("properties", {})
    fields = props.get("fields", {})

    # Owner info
    owner = (fields.get("owner") or "").strip()
    owner2 = (fields.get("owner2") or "").strip() or None

    # Mailing address
    mail_addr = (fields.get("mailadd") or "").strip()
    mail_city = (fields.get("mail_city") or "").strip()
    mail_state = (fields.get("mail_state2") or "").strip()
    mail_zip = (fields.get("mail_zip") or "").strip()

    mailing = None
    if mail_addr:
        mailing = mail_addr
        if mail_city:
            mailing += ", " + mail_city
        if mail_state:
            mailing += " " + mail_state
        if mail_zip:
            mailing += " " + mail_zip

    # APN
    apn = (fields.get("parcelnumb") or fields.get("parcelnumb_no_formatting") or "").strip()

    # Assessed values
    assessed_total = fields.get("parval")
    assessed_land = fields.get("landval")
    assessed_impr = fields.get("improvval")
    assessed_prev = fields.get("parval_prev") or fields.get("prvval")

    # Compute improvement if not directly available
    if assessed_total and assessed_land and not assessed_impr:
        try:
            assessed_impr = float(assessed_total) - float(assessed_land)
        except Exception:
            pass

    # YoY change
    yoy_change = None
    if assessed_total and assessed_prev:
        try:
            assessed_total_f = float(assessed_total)
            assessed_prev_f = float(assessed_prev)
            if assessed_prev_f > 0:
                yoy_change = round(((assessed_total_f - assessed_prev_f) / assessed_prev_f) * 100, 1)
        except Exception:
            pass

    # Building info
    bldg_sf = fields.get("sqft") or fields.get("ll_bldg_footprint_sqft")
    year_built = fields.get("yearbuilt")
    lot_acres = fields.get("gisacre") or fields.get("lotsize")

    # Use/class
    use_desc = (fields.get("usedesc") or fields.get("usecode") or "").strip()
    zoning = (fields.get("zoning") or "").strip()
    prop_class = (fields.get("propclass") or fields.get("classcode") or "").strip()

    # Situs address
    site_addr = (
        fields.get("address") or
        " ".join(filter(None, [
            str(fields.get("saddno") or ""),
            str(fields.get("saddpref") or ""),
            str(fields.get("saddstr") or ""),
            str(fields.get("saddsttyp") or ""),
        ])).strip()
    )

    # Geography — try multiple field paths Regrid uses
    state = (
        fields.get("state_abbr") or
        props.get("state_abbr") or
        fields.get("state2") or
        props.get("state2") or
        ""
    ).upper()
    county = (fields.get("county") or props.get("county") or "").strip()

    # Derive state from mailing address if still missing and owner is likely in-state
    if not state and mail_state:
        state = mail_state.upper()

    # Absentee / out-of-state detection
    site_city_raw = (
        fields.get("saddcity") or
        fields.get("scity") or
        (site_addr.split(",")[1].strip() if "," in site_addr else "")
    )
    out_of_state = (mail_state.upper() != state.upper() and bool(mail_state) and bool(state))
    absentee = bool(mail_city and site_city_raw and
                    mail_city.upper() != site_city_raw.upper()) if mail_city else False

    # Source URL (Regrid map)
    ll_uuid = fields.get("ll_uuid") or ""
    map_url = f"https://app.regrid.com/us/{state.lower()}/{county.lower().replace(' ', '-')}" if state and county else "https://app.regrid.com"

    result = {
        "source": "Regrid",
        "source_url": map_url,
        "apn": apn or None,
        "owner_name": owner or None,
        "owner_name2": owner2,
        "owner_mailing": mailing,
        "owner_city": mail_city or None,
        "owner_state": mail_state or None,
        "owner_zip": mail_zip or None,
        "property_address": site_addr or None,
        "county": county or None,
        "state": state or None,
        "use_description": use_desc or None,
        "zoning": zoning or None,
        "property_class": prop_class or None,
        "building_sf": bldg_sf,
        "year_built": year_built,
        "lot_acres": lot_acres,
        "assessed_total": assessed_total,
        "assessed_land": assessed_land,
        "assessed_improvement": assessed_impr,
        "assessed_prev": assessed_prev,
        "assessed_yoy_pct": yoy_change,
        "absentee_owner": absentee,
        "out_of_state_owner": out_of_state,
        "tax_delinquent": False,
        "regrid_uuid": ll_uuid or None,
    }
    return result


def search_by_address(address, state=None, county=None, limit=1):
    """
    Search Regrid by property address.
    National coverage — works for any US address on paid plan.
    Trial token: restricted to 7 counties (DFW area covered).

    Returns: parsed parcel dict or error dict
    """
    if not REGRID_TOKEN:
        return {"error": "REGRID_API_KEY not configured", "source": "Regrid"}

    if not address:
        return {"error": "No address provided", "source": "Regrid"}

    params = _params(query=address, limit=limit)
    # Optional path filter to disambiguate (e.g., "us/tx/dallas")
    if state and county:
        params["path"] = "us/{}/{}".format(
            state.lower(),
            county.lower().replace(" ", "-")
        )
    elif state:
        params["path"] = "us/{}".format(state.lower())

    try:
        resp = requests.get(
            "{}/parcels/address".format(BASE_URL),
            params=params,
            headers=HEADERS,
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
    except requests.exceptions.Timeout:
        return {"error": "Regrid API timeout", "source": "Regrid"}
    except requests.exceptions.HTTPError as e:
        status = e.response.status_code if e.response else 0
        if status == 401:
            return {"error": "Regrid API key invalid or expired", "source": "Regrid"}
        if status == 403 and (state or county):
            # Trial key blocks path= filter — retry without geographic restriction
            return search_by_address(address, state=None, county=None, limit=limit)
        if status == 403:
            return {"error": "Regrid: address outside trial coverage area", "source": "Regrid"}
        return {"error": "Regrid HTTP {}: {}".format(status, str(e)), "source": "Regrid"}
    except Exception as e:
        return {"error": str(e) or repr(e), "source": "Regrid"}

    features = data.get("parcels", {}).get("features", [])
    if not features:
        return {"error": "No parcel found for this address", "source": "Regrid", "query": address}

    parsed = _parse_parcel(features[0])
    if not parsed:
        return {"error": "Failed to parse Regrid response", "source": "Regrid"}

    return parsed


def search_by_point(lat, lng, limit=1):
    """
    Search Regrid by lat/lon coordinates.
    Useful after geocoding — pinpoint exact parcel.
    """
    if not REGRID_TOKEN:
        return {"error": "REGRID_API_KEY not configured", "source": "Regrid"}

    try:
        resp = requests.get(
            "{}/parcels/point".format(BASE_URL),
            params=_params(lat=lat, lon=lng, limit=limit),
            headers=HEADERS,
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
    except requests.exceptions.Timeout:
        return {"error": "Regrid API timeout", "source": "Regrid"}
    except requests.exceptions.HTTPError as e:
        status = e.response.status_code if e.response else 0
        if status == 403:
            return {"error": "Regrid trial — coordinates outside covered counties", "source": "Regrid"}
        return {"error": "Regrid HTTP {}: {}".format(status, str(e)), "source": "Regrid"}
    except Exception as e:
        return {"error": str(e) or repr(e), "source": "Regrid"}

    features = data.get("parcels", {}).get("features", [])
    if not features:
        return {"error": "No parcel found at these coordinates", "source": "Regrid"}

    return _parse_parcel(features[0]) or {"error": "Parse failed", "source": "Regrid"}


def search_by_apn(apn, state=None, county=None):
    """Search Regrid by Assessor Parcel Number."""
    if not REGRID_TOKEN:
        return {"error": "REGRID_API_KEY not configured", "source": "Regrid"}

    params = _params(parcelnumb=apn)
    if state and county:
        params["path"] = "us/{}/{}".format(state.lower(), county.lower().replace(" ", "-"))
    elif state:
        params["path"] = "us/{}".format(state.lower())

    try:
        resp = requests.get(
            "{}/parcels/apn".format(BASE_URL),
            params=params,
            headers=HEADERS,
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
    except requests.exceptions.HTTPError as e:
        status = e.response.status_code if e.response else 0
        return {"error": "Regrid HTTP {}".format(status), "source": "Regrid"}
    except Exception as e:
        return {"error": str(e), "source": "Regrid"}

    features = data.get("parcels", {}).get("features", [])
    if not features:
        return {"error": "APN not found", "source": "Regrid"}

    return _parse_parcel(features[0]) or {"error": "Parse failed", "source": "Regrid"}


def search_nearby(lat, lng, radius_miles=0.3, limit=8, same_use=True):
    """
    Find nearby parcels within radius_miles using bounding box.
    Used to build a basic comparable sales/value table.
    
    Returns list of parsed parcel dicts (excluding the subject parcel itself).
    """
    if not REGRID_TOKEN:
        return []

    # Convert miles to approximate degrees (at ~32°N latitude)
    deg_per_mile = 1 / 69.0
    delta = radius_miles * deg_per_mile

    min_lng = lng - delta * 1.15  # longitude degrees are shorter at 32°N
    max_lng = lng + delta * 1.15
    min_lat = lat - delta
    max_lat = lat + delta

    try:
        resp = requests.get(
            "{}/parcels/bbox".format(BASE_URL),
            params=_params(bbox="{},{},{},{}".format(min_lng, min_lat, max_lng, max_lat), limit=limit + 1),
            headers=HEADERS,
            timeout=20,
        )
        resp.raise_for_status()
        data = resp.json()
    except requests.exceptions.Timeout:
        return []
    except requests.exceptions.HTTPError as e:
        return []
    except Exception:
        return []

    features = data.get("parcels", {}).get("features", [])
    results = []
    for feat in features:
        parsed = _parse_parcel(feat)
        if not parsed:
            continue
        # Skip parcels without assessed value (not useful as comps)
        if not parsed.get("assessed_total"):
            continue
        # Skip the subject property itself (exact coords match)
        props = feat.get("properties", {})
        fields = props.get("fields", {})
        try:
            feat_lat = feat["geometry"]["coordinates"][0][0][1]
            feat_lng = feat["geometry"]["coordinates"][0][0][0]
            if abs(feat_lat - lat) < 0.0001 and abs(feat_lng - lng) < 0.0001:
                continue
        except Exception:
            pass
        results.append(parsed)
        if len(results) >= limit:
            break

    return results
