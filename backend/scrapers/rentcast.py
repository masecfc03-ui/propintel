"""
RentCast Property Data API — National Coverage
https://www.rentcast.io/api / https://developers.rentcast.io

140M+ properties nationwide. Free tier: 50 req/mo.
Auth: X-Api-Key: <key> header.

Set RENTCAST_API_KEY env var to activate.

Endpoints used:
  GET /v1/properties?address=<addr>      — full property record (owner, beds, baths, SF, year)
  GET /v1/avm/value?address=<addr>       — AVM estimate
  GET /v1/avm/rent/long-term?address=<addr> — rent estimate

Replaces county-level scrapers for any US address not covered by direct CAD integration.
"""
import os
import json
import logging
import urllib.request
import urllib.parse
import urllib.error

log = logging.getLogger(__name__)

BASE_URL = "https://api.rentcast.io/v1"
API_KEY = os.environ.get("RENTCAST_API_KEY", "")
TIMEOUT = 15


def _get(path, params=None):
    """Authenticated GET to RentCast API."""
    if not API_KEY:
        return {"error": "RENTCAST_API_KEY not configured"}
    qs = urllib.parse.urlencode(params or {})
    url = "{}/{}?{}".format(BASE_URL, path.lstrip("/"), qs) if qs else "{}/{}".format(BASE_URL, path.lstrip("/"))
    req = urllib.request.Request(url, headers={
        "X-Api-Key": API_KEY,
        "Accept": "application/json",
    })
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        body = e.read().decode(errors="ignore")
        log.warning("RentCast HTTP %s on %s: %s", e.code, path, body[:200])
        return {"error": "HTTP {}".format(e.code), "detail": body[:200]}
    except Exception as ex:
        log.warning("RentCast error on %s: %s", path, ex)
        return {"error": str(ex)}


def get_property(address):
    """
    Fetch full property record from RentCast by address string.
    Returns PropIntel-compatible parcel dict or error.

    address: "123 Main St, Dallas, TX 75201" — full address preferred
    """
    if not API_KEY:
        return {"available": False, "error": "RENTCAST_API_KEY not set"}

    raw = _get("properties", {"address": address, "limit": 1})

    # RentCast returns a list or single object depending on endpoint
    if isinstance(raw, list):
        if not raw:
            return {"available": False, "error": "No property found: {}".format(address)}
        prop = raw[0]
    elif isinstance(raw, dict):
        if raw.get("error"):
            return {"available": False, "error": raw["error"]}
        prop = raw
    else:
        return {"available": False, "error": "Unexpected response format"}

    # Normalize to PropIntel parcel dict
    owner = (prop.get("ownerName") or "").strip()
    owner2 = (prop.get("ownerName2") or "").strip() or None
    mailing = prop.get("mailAddress") or prop.get("ownerAddress") or None
    if isinstance(mailing, dict):
        m = mailing
        mailing = "{}, {}, {} {}".format(
            m.get("addressLine1", ""), m.get("city", ""),
            m.get("state", ""), m.get("zipCode", "")
        ).strip(", ")

    bedrooms = prop.get("bedrooms")
    bathrooms = prop.get("bathrooms")
    building_sf = prop.get("squareFootage") or prop.get("livingArea")
    lot_sf = prop.get("lotSize")
    yr_built = prop.get("yearBuilt")
    prop_type = prop.get("propertyType") or prop.get("type")
    assessed = prop.get("assessedValue") or prop.get("totalAssessedValue")
    market_val = prop.get("lastSalePrice") or assessed

    # Owner mailing info
    owner_city = ""
    owner_state = ""
    owner_zip = ""
    if isinstance(prop.get("mailAddress"), dict):
        owner_city = prop["mailAddress"].get("city", "")
        owner_state = prop["mailAddress"].get("state", "")
        owner_zip = prop["mailAddress"].get("zipCode", "")

    prop_city = prop.get("city", "")
    prop_state = prop.get("state", "")
    prop_zip = prop.get("zipCode", "")

    absentee = bool(owner_city and prop_city and owner_city.upper() != prop_city.upper())
    out_of_state = bool(owner_state and prop_state and owner_state.upper() != prop_state.upper())

    return {
        "available": True,
        "source": "RentCast",
        "source_url": "https://www.rentcast.io",
        "apn": prop.get("id") or prop.get("parcelId") or prop.get("fipsCode"),
        "owner_name": owner,
        "owner_name2": owner2,
        "owner_mailing": mailing,
        "owner_city": owner_city,
        "owner_state": owner_state,
        "owner_zip": owner_zip,
        "property_address": prop.get("addressLine1") or prop.get("address"),
        "property_city": prop_city,
        "property_state": prop_state,
        "property_zip": prop_zip,
        "bedrooms": bedrooms,
        "bathrooms": bathrooms,
        "building_sf": building_sf,
        "lot_sf": lot_sf,
        "year_built": yr_built,
        "use_description": prop_type,
        "assessed_total": assessed,
        "assessed_land": None,
        "assessed_improvement": None,
        "assessed_prev": None,
        "assessed_yoy_pct": None,
        "absentee_owner": absentee,
        "out_of_state_owner": out_of_state,
        "tax_delinquent": False,
        "_raw": prop,
    }


def get_avm(address):
    """Get RentCast AVM estimate for an address."""
    if not API_KEY:
        return {"available": False}
    raw = _get("avm/value", {"address": address})
    if raw.get("error") or not raw.get("price"):
        return {"available": False, "error": raw.get("error", "No AVM available")}
    price = raw.get("price")
    low = raw.get("priceRangeLow")
    high = raw.get("priceRangeHigh")
    return {
        "available": True,
        "value": price,
        "value_low": low,
        "value_high": high,
        "value_fmt": "${:,.0f}".format(price) if price else None,
        "range_fmt": "${:,.0f} – ${:,.0f}".format(low, high) if low and high else None,
        "source": "RentCast AVM",
    }


def is_available():
    """Check if RentCast is configured."""
    return bool(API_KEY)
