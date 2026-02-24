"""
Realie.ai Property Data API scraper
https://app.realie.ai / https://docs.realie.ai

Provides: property details, sold comps, AVM, owner search
Free tier: 25 req/mo | Tier 1: $50/mo / 1,250 req | overage $0.05/req
Auth: Authorization: Bearer <key>

Set REALIE_API_KEY env var to activate.

Endpoints:
  GET /public/premium/comparables/    — sold comps by lat/lng + radius
  GET /public/premium/owner-search/   — owner portfolio lookup
  GET /public/property-search/        — paginated area search
  Address lookup: tries multiple paths at startup
"""

import os
import logging
import json
import time
import urllib.request
import urllib.parse

log = logging.getLogger(__name__)

BASE_URL  = "https://app.realie.ai/api"
API_KEY   = os.environ.get("REALIE_API_KEY", "")
TIMEOUT   = 15
MAX_RETRY = 2

# ─── INTERNAL HELPERS ─────────────────────────────────────────────────────────

def _get(path: str, params: dict) -> dict:
    """Make authenticated GET request to Realie API."""
    if not API_KEY:
        return {"error": "REALIE_API_KEY not configured"}

    qs  = urllib.parse.urlencode({k: v for k, v in params.items() if v is not None})
    url = f"{BASE_URL}/{path.lstrip('/')}?{qs}"
    req = urllib.request.Request(
        url,
        headers={"Authorization": f"Bearer {API_KEY}", "Accept": "application/json"},
        method="GET",
    )

    for attempt in range(MAX_RETRY):
        try:
            with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
                body = resp.read().decode("utf-8", errors="ignore")
                return json.loads(body)
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", errors="ignore")
            log.warning("Realie HTTP %s on %s: %s", e.code, path, body[:200])
            if e.code == 429:
                time.sleep(2 ** attempt)
                continue
            if e.code == 401:
                return {"error": "Invalid or inactive Realie API key"}
            return {"error": f"HTTP {e.code}", "detail": body[:300]}
        except Exception as ex:
            log.warning("Realie error on %s: %s", path, ex)
            if attempt < MAX_RETRY - 1:
                time.sleep(1)
                continue
            return {"error": str(ex)}

    return {"error": "Max retries exceeded"}


def _address_lookup(address: str) -> dict:
    """
    Try known Realie address lookup endpoints.
    Realie docs list 'Address Lookup' but the exact path needs confirmation.
    Falls back gracefully if not found.
    """
    candidates = [
        ("public/property/", {"address": address}),
        ("public/lookup/",   {"address": address}),
        ("public/search/",   {"address": address, "limit": 1}),
        ("public/properties/", {"address": address}),
    ]
    for path, params in candidates:
        result = _get(path, params)
        # If we get a non-error response with actual data, use it
        if result and not result.get("error"):
            return result
        # If 404, try next candidate
        if result.get("error", "").startswith("HTTP 404"):
            continue
        # Any other error (auth, etc.) — return it immediately
        return result

    return {"error": "Address lookup endpoint not found — check Realie docs for current path"}


# ─── PUBLIC FUNCTIONS (same interface as attom.py) ───────────────────────────

def get_property_detail(address: str) -> dict:
    """
    Get full property detail from Realie by address.
    Returns parcel, valuation, building characteristics.
    """
    data = _address_lookup(address)
    if data.get("error"):
        return {"available": False, **data}

    try:
        # Realie may return list or single object
        prop = data if isinstance(data, dict) and data.get("parcelId") else \
               (data[0] if isinstance(data, list) and data else data)

        assessed   = prop.get("assessedValue") or prop.get("totalAssessedValue")
        market_val = prop.get("marketValue") or prop.get("totalMarketValue")
        bldg_sf    = prop.get("buildingArea") or prop.get("squareFeet")

        return {
            "available":      True,
            "parcel_id":      prop.get("parcelId"),
            "address":        prop.get("addressFull") or prop.get("address"),
            "city":           prop.get("city"),
            "state":          prop.get("state"),
            "zip":            prop.get("zipCode"),
            "county":         prop.get("county"),
            "year_built":     prop.get("yearBuilt"),
            "building_sf":    bldg_sf,
            "lot_sf":         prop.get("lotArea") or prop.get("lotSquareFeet"),
            "use_type":       prop.get("propertyType") or prop.get("landUse"),
            "beds":           prop.get("bedrooms"),
            "baths":          prop.get("bathrooms"),
            "assessed_value": assessed,
            "assessed_fmt":   f"${assessed:,.0f}" if assessed else None,
            "market_value":   market_val,
            "market_fmt":     f"${market_val:,.0f}" if market_val else None,
            "owner_name":     prop.get("ownerName") or prop.get("owner"),
            "last_sale_date": prop.get("lastSaleDate") or prop.get("saleDate"),
            "last_sale_price":prop.get("lastSalePrice") or prop.get("salePrice"),
            "source":         "Realie",
            "_raw":           prop,
        }
    except (KeyError, IndexError, TypeError) as e:
        log.warning("Realie property parse error: %s | raw: %s", e, str(data)[:300])
        return {"available": False, "error": str(e)}


def get_avm(address: str, lat: float = None, lng: float = None) -> dict:
    """
    Get Realie AVM (automated valuation model).
    Realie includes AVM in property detail or comparables stats.
    Returns value estimate + confidence range.
    """
    # AVM is typically in the property detail response
    detail = get_property_detail(address)
    if not detail.get("available"):
        return {"available": False, "error": detail.get("error", "Property not found")}

    raw = detail.get("_raw", {})

    # Look for AVM fields — Realie may name these differently
    avm_value = (
        raw.get("estimatedValue") or
        raw.get("avmValue") or
        raw.get("avm") or
        raw.get("automatedValuation") or
        detail.get("market_value")  # fallback to market value
    )
    avm_low   = raw.get("estimatedValueLow") or raw.get("avmLow")
    avm_high  = raw.get("estimatedValueHigh") or raw.get("avmHigh")
    conf      = raw.get("avmConfidence") or raw.get("confidenceScore")

    if not avm_value:
        return {"available": False, "error": "AVM not returned by Realie for this address"}

    return {
        "available":        True,
        "value":            avm_value,
        "value_low":        avm_low,
        "value_high":       avm_high,
        "value_fmt":        f"${avm_value:,.0f}",
        "range_fmt":        f"${avm_low:,.0f} – ${avm_high:,.0f}" if avm_low and avm_high else None,
        "confidence_score": conf,
        "source":           "Realie AVM",
    }


def get_sold_comps(address: str, zipcode: str = "",
                   lat: float = None, lng: float = None,
                   radius_miles: float = 1.0,
                   months_back: int = 18,
                   max_results: int = 10) -> dict:
    """
    Get sold comparable properties within radius over the past N months.
    Requires lat/lng — use geocode.py to get them from address.

    Returns list of comps with address, price, SF, sold date, $/SF.
    """
    if not lat or not lng:
        # Try to geocode from address
        try:
            from .geocode import geocode_address
            geo = geocode_address(address)
            lat = geo.get("lat")
            lng = geo.get("lng")
        except Exception:
            pass

    if not lat or not lng:
        return {
            "available": False,
            "comps": [],
            "error": "lat/lng required for Realie comps — geocoding failed",
        }

    params = {
        "latitude":   round(lat, 6),
        "longitude":  round(lng, 6),
        "radius":     radius_miles,
        "timeFrame":  months_back,
        "maxResults": min(max_results, 50),
    }

    data = _get("public/premium/comparables/", params)
    if data.get("error"):
        return {"available": False, "comps": [], "error": data["error"]}

    # Realie returns a list of properties or dict with results key
    raw_props = data if isinstance(data, list) else \
                data.get("results") or data.get("comparables") or data.get("properties") or []

    if not raw_props:
        return {"available": False, "comps": [], "error": "No comps returned"}

    comps = []
    for prop in raw_props:
        try:
            sale_price = (
                prop.get("salePrice") or
                prop.get("lastSalePrice") or
                prop.get("soldPrice") or
                prop.get("price")
            )
            if not sale_price:
                continue

            sale_date = (
                prop.get("saleDate") or
                prop.get("lastSaleDate") or
                prop.get("soldDate")
            )

            bldg_sf = (
                prop.get("buildingArea") or
                prop.get("squareFeet") or
                prop.get("livingArea")
            )

            price_per_sf = round(sale_price / bldg_sf, 0) if bldg_sf and bldg_sf > 0 else None

            addr_str = (
                prop.get("addressFull") or
                prop.get("address") or
                f"{prop.get('streetNumber','')} {prop.get('street','')}".strip()
            )

            comps.append({
                "address":      addr_str,
                "city":         prop.get("city", ""),
                "state":        prop.get("state", ""),
                "zip":          prop.get("zipCode", ""),
                "sale_amount":  sale_price,
                "sale_fmt":     f"${sale_price:,.0f}",
                "sale_date":    sale_date,
                "beds":         prop.get("bedrooms"),
                "baths":        prop.get("bathrooms"),
                "building_sf":  bldg_sf,
                "sf_fmt":       f"{int(bldg_sf):,} SF" if bldg_sf else None,
                "price_per_sf": price_per_sf,
                "psf_fmt":      f"${price_per_sf:,.0f}/SF" if price_per_sf else None,
                "year_built":   prop.get("yearBuilt"),
                "use_type":     prop.get("propertyType") or prop.get("landUse"),
                "distance_mi":  prop.get("distance") or prop.get("distanceMiles"),
            })
        except Exception as ex:
            log.debug("Realie comp parse skip: %s", ex)
            continue

    comps.sort(key=lambda x: x.get("sale_date") or "", reverse=True)

    # Stats
    prices   = [c["sale_amount"] for c in comps if c["sale_amount"]]
    psf_vals = [c["price_per_sf"] for c in comps if c.get("price_per_sf")]

    stats = {}
    if prices:
        stats["comp_count"]       = len(prices)
        stats["median_price"]     = sorted(prices)[len(prices) // 2]
        stats["avg_price"]        = round(sum(prices) / len(prices), 0)
        stats["low_price"]        = min(prices)
        stats["high_price"]       = max(prices)
        stats["median_price_fmt"] = f"${stats['median_price']:,.0f}"
        stats["avg_price_fmt"]    = f"${stats['avg_price']:,.0f}"
        stats["price_range_fmt"]  = f"${stats['low_price']:,.0f} – ${stats['high_price']:,.0f}"
    if psf_vals:
        stats["median_psf"]     = round(sorted(psf_vals)[len(psf_vals) // 2], 0)
        stats["median_psf_fmt"] = f"${stats['median_psf']:,.0f}/SF"

    return {
        "available":    len(comps) > 0,
        "comps":        comps,
        "stats":        stats,
        "radius_miles": radius_miles,
        "months_back":  months_back,
        "source":       "Realie",
    }


def get_ownership_history(address: str) -> dict:
    """
    Get deed/sale history from Realie property detail.
    Falls back to last sale date/price if full history not available.
    """
    detail = get_property_detail(address)
    if not detail.get("available"):
        return {"available": False, "history": [], "error": detail.get("error")}

    raw   = detail.get("_raw", {})
    hist  = raw.get("saleHistory") or raw.get("transactionHistory") or raw.get("ownershipHistory") or []

    history = []
    if isinstance(hist, list):
        for h in hist:
            sale_price = h.get("salePrice") or h.get("price") or h.get("amount")
            history.append({
                "sale_date":     h.get("saleDate") or h.get("date"),
                "sale_amount":   sale_price,
                "sale_fmt":      f"${sale_price:,.0f}" if sale_price else None,
                "buyer_name":    h.get("buyerName") or h.get("buyer"),
                "document_type": h.get("documentType") or h.get("transactionType"),
            })
        history.sort(key=lambda x: x.get("sale_date") or "", reverse=True)

    # Fallback: build single entry from last sale data
    if not history and detail.get("last_sale_date"):
        p = detail.get("last_sale_price")
        history = [{
            "sale_date":   detail["last_sale_date"],
            "sale_amount": p,
            "sale_fmt":    f"${p:,.0f}" if p else None,
            "buyer_name":  detail.get("owner_name"),
            "document_type": "Deed",
        }]

    hold_years = None
    if history and history[0].get("sale_date"):
        from datetime import datetime
        try:
            bought     = datetime.strptime(history[0]["sale_date"][:10], "%Y-%m-%d")
            hold_years = round((datetime.now() - bought).days / 365.25, 1)
        except ValueError:
            pass

    return {
        "available":  len(history) > 0,
        "history":    history,
        "hold_years": hold_years,
        "source":     "Realie",
    }


def get_owner_portfolio(owner_name: str, state: str = "TX", limit: int = 10) -> dict:
    """
    Search Realie's owner database — find all properties owned by this entity.
    Great for LLC/entity lookup to understand total portfolio size.
    """
    params = {
        "name":   owner_name,
        "state":  state,
        "limit":  limit,
    }
    data = _get("public/premium/owner-search/", params)
    if data.get("error"):
        return {"available": False, "properties": [], "error": data["error"]}

    raw = data if isinstance(data, list) else \
          data.get("results") or data.get("properties") or []

    properties = []
    for prop in raw:
        assessed = prop.get("assessedValue") or prop.get("totalAssessedValue")
        properties.append({
            "address":        prop.get("addressFull") or prop.get("address"),
            "city":           prop.get("city"),
            "state":          prop.get("state"),
            "zip":            prop.get("zipCode"),
            "assessed_value": assessed,
            "assessed_fmt":   f"${assessed:,.0f}" if assessed else None,
            "use_type":       prop.get("propertyType") or prop.get("landUse"),
        })

    return {
        "available":        len(properties) > 0,
        "owner_name":       owner_name,
        "portfolio_count":  len(properties),
        "properties":       properties,
        "source":           "Realie",
    }
