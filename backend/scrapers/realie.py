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
        headers={"Authorization": API_KEY, "Accept": "application/json"},
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


def _address_lookup(address: str, state: str = "TX", zip_code: str = "") -> dict:
    """
    Look up a property by address using Realie's property search endpoint.
    Endpoint: GET /public/property/search/?address=<street>&state=<ST>&zipCode=<zip>&limit=1

    address: full address or just street portion (e.g. "3229 FOREST LN")
    state:   2-letter state code (required by Realie API)
    zip_code: optional, improves match accuracy
    """
    # Extract just the street portion if full address given
    # "3229 Forest Ln, Garland TX 75042" → "3229 FOREST LN"
    import re as _re
    street_only = _re.split(r",\s*", address)[0].strip().upper()

    # Also try to extract state from address if not provided
    if not state or state == "TX":
        state_match = _re.search(r"\b([A-Z]{2})\s+\d{5}", address.upper())
        if state_match:
            state = state_match.group(1)

    # Extract zip if not provided
    if not zip_code:
        zip_match = _re.search(r"\b(\d{5})\b", address)
        if zip_match:
            zip_code = zip_match.group(1)

    params = {
        "address": street_only,
        "state":   state,
        "limit":   1,
    }
    if zip_code:
        params["zipCode"] = zip_code

    result = _get("public/property/search/", params)
    if result.get("error"):
        return result

    props = result.get("properties", [])
    if not props:
        return {"error": f"No property found for: {address}"}

    return props[0]  # return first match directly


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
        # _address_lookup returns the property dict directly
        prop = data

        assessed   = prop.get("totalAssessedValue") or prop.get("assessedValue")
        market_val = prop.get("totalMarketValue") or prop.get("marketValue")
        bldg_sf    = prop.get("buildingArea") or prop.get("squareFeet")
        lot_sf     = prop.get("landArea") or prop.get("lotArea") or prop.get("lotSquareFeet")

        return {
            "available":      True,
            "parcel_id":      prop.get("parcelId"),
            "address":        prop.get("address"),
            "city":           prop.get("city"),
            "state":          prop.get("state"),
            "zip":            prop.get("zipCode"),
            "county":         prop.get("county"),
            "year_built":     prop.get("yearBuilt"),
            "building_sf":    bldg_sf,
            "lot_sf":         lot_sf,
            "acres":          prop.get("acres"),
            "beds":           prop.get("totalBedrooms"),
            "baths":          prop.get("totalBathrooms"),
            "stories":        prop.get("stories"),
            "pool":           prop.get("pool"),
            "garage_count":   prop.get("garageCount"),
            "assessed_value": assessed,
            "assessed_fmt":   f"${assessed:,.0f}" if assessed else None,
            "tax_value":      prop.get("taxValue"),
            "tax_year":       prop.get("taxYear"),
            "market_value":   market_val,
            "market_fmt":     f"${market_val:,.0f}" if market_val else None,
            "owner_name":     prop.get("ownerName"),
            "last_sale_date": prop.get("lastSaleDate") or prop.get("saleDate"),
            "last_sale_price":prop.get("lastSalePrice") or prop.get("salePrice"),
            "subdivision":    prop.get("subdivision"),
            "legal_desc":     prop.get("legalDesc"),
            "source":         "Realie",
            "_raw":           prop,
        }
    except (KeyError, IndexError, TypeError) as e:
        log.warning("Realie property parse error: %s | raw: %s", e, str(data)[:300])
        return {"available": False, "error": str(e)}


def get_avm(address: str, lat: float = None, lng: float = None) -> dict:
    """
    Get Realie AVM from property detail.
    Realie field: modelValue (AVM), modelValueMin, modelValueMax.
    Also returns equity + lien data from the same call.
    """
    detail = get_property_detail(address)
    if not detail.get("available"):
        return {"available": False, "error": detail.get("error", "Property not found")}

    raw = detail.get("_raw", {})

    # Realie AVM fields (confirmed from API response)
    avm_value = raw.get("modelValue")
    avm_low   = raw.get("modelValueMin")
    avm_high  = raw.get("modelValueMax")

    # Fall back to totalMarketValue if model value not present
    if not avm_value:
        avm_value = raw.get("totalMarketValue")

    if not avm_value:
        return {"available": False, "error": "AVM not available for this property"}

    equity    = raw.get("equityCurrentEstBal")
    lien_bal  = raw.get("totalLienBalance")
    ltv       = raw.get("LTVCurrentEstCombined")

    return {
        "available":        True,
        "value":            avm_value,
        "value_low":        avm_low,
        "value_high":       avm_high,
        "value_fmt":        f"${avm_value:,.0f}",
        "range_fmt":        f"${avm_low:,.0f} – ${avm_high:,.0f}" if avm_low and avm_high else None,
        "confidence_score": None,  # Realie doesn't expose confidence score
        "equity_estimate":  equity,
        "equity_fmt":       f"${equity:,.0f}" if equity else None,
        "lien_balance":     lien_bal,
        "lien_fmt":         f"${lien_bal:,.0f}" if lien_bal else None,
        "ltv_pct":          ltv,
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
        # Fallback: use Realie's own property search to get lat/lng
        detail = _address_lookup(address)
        if not detail.get("error"):
            lat = detail.get("latitude") or detail.get("lat")
            lng = detail.get("longitude") or detail.get("lng")

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

    # Realie returns {"comparables": [...], "metadata": {...}}
    raw_props = data.get("comparables") or data.get("properties") or \
                (data if isinstance(data, list) else [])

    if not raw_props:
        return {"available": False, "comps": [], "error": "No comps returned"}

    comps = []
    for prop in raw_props:
        try:
            # Realie confirmed field names from API response
            sale_price = prop.get("transferPrice")
            if not sale_price:
                continue

            # transferDate format: "20250514" → normalize to "2025-05-14"
            raw_date  = prop.get("transferDate") or prop.get("recordingDate")
            sale_date = None
            if raw_date and len(str(raw_date)) == 8:
                d = str(raw_date)
                sale_date = f"{d[:4]}-{d[4:6]}-{d[6:]}"
            elif raw_date:
                sale_date = str(raw_date)[:10]

            bldg_sf      = prop.get("buildingArea")
            price_per_sf = round(sale_price / bldg_sf, 0) if bldg_sf and bldg_sf > 0 else None
            addr_str     = prop.get("addressFull") or prop.get("address", "")
            avm          = prop.get("modelValue")

            # Filter out commercial / non-residential properties
            if prop.get("residential") is False:
                continue
            if sale_price > 5_000_000:
                continue
            if bldg_sf and bldg_sf > 20_000:
                continue

            comps.append({
                "address":        addr_str,
                "city":           prop.get("city", ""),
                "state":          prop.get("state", ""),
                "zip":            prop.get("zipCode", ""),
                "sale_amount":    sale_price,
                "sale_fmt":       f"${sale_price:,.0f}",
                "sale_date":      sale_date,
                "beds":           prop.get("totalBedrooms"),
                "baths":          prop.get("totalBathrooms"),
                "building_sf":    bldg_sf,
                "sf_fmt":         f"{int(bldg_sf):,} SF" if bldg_sf else None,
                "price_per_sf":   price_per_sf,
                "psf_fmt":        f"${price_per_sf:,.0f}/SF" if price_per_sf else None,
                "year_built":     prop.get("yearBuilt"),
                "acres":          prop.get("acres"),
                "avm":            avm,
                "avm_fmt":        f"${avm:,.0f}" if avm else None,
                "lien_balance":   prop.get("totalLienBalance"),
                "equity_est":     prop.get("equityCurrentEstBal"),
                "owner_name":     prop.get("ownerName"),
                "owner_state":    prop.get("ownerState"),  # out-of-state signal
                "lender_name":    prop.get("lenderName"),
                "subdivision":    prop.get("subdivision"),
                "owner_count":    prop.get("ownerParcelCount"),  # portfolio size
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

    raw  = detail.get("_raw", {})
    # Realie: ownership history is in the "transfers" array
    hist = raw.get("transfers") or []

    def _norm_date(d):
        """Normalize Realie date format 20250514 → 2025-05-14."""
        d = str(d) if d else ""
        if len(d) == 8 and d.isdigit():
            return f"{d[:4]}-{d[4:6]}-{d[6:]}"
        return d[:10] if d else None

    history = []
    if isinstance(hist, list):
        for h in hist:
            price    = h.get("transferPrice")
            raw_date = h.get("transferDate") or h.get("recordingDate")
            history.append({
                "sale_date":     _norm_date(raw_date),
                "sale_amount":   price,
                "sale_fmt":      f"${price:,.0f}" if price else None,
                "buyer_name":    h.get("grantee"),
                "seller_name":   h.get("grantor"),
                "document_type": h.get("transferDocType"),
                "doc_number":    h.get("transferDocNum"),
            })
        history.sort(key=lambda x: x.get("sale_date") or "", reverse=True)

    # Fallback: build from current transfer data on the property itself
    if not history and raw.get("transferPrice"):
        price    = raw.get("transferPrice")
        raw_date = raw.get("transferDate") or raw.get("recordingDate")
        history = [{
            "sale_date":     _norm_date(raw_date),
            "sale_amount":   price,
            "sale_fmt":      f"${price:,.0f}" if price else None,
            "buyer_name":    raw.get("ownerName"),
            "document_type": raw.get("transferDocType"),
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


def get_mortgage_lien(address: str, zipcode: str = "") -> dict:
    """
    Get mortgage/lien/equity data from Realie property detail.
    Fields: totalLienBalance, LTVCurrentEstCombined, equityCurrentEstBal, lenderName.
    """
    detail = get_property_detail(address)
    if not detail.get("available"):
        return {"available": False, "error": detail.get("error")}

    raw = detail.get("_raw", {})

    lien_bal  = raw.get("totalLienBalance")
    equity    = raw.get("equityCurrentEstBal")
    ltv       = raw.get("LTVCurrentEstCombined")
    ltv_purch = raw.get("LTVPurchase")
    lender    = raw.get("lenderName")
    lien_cnt  = raw.get("totalLienCount")
    fin_hist  = raw.get("totalFinancingHistCount")

    if lien_bal is None and equity is None:
        return {"available": False, "error": "No lien/equity data for this property"}

    return {
        "available":          True,
        "open_lien_balance":  lien_bal,
        "open_lien_fmt":      f"${lien_bal:,.0f}" if lien_bal else None,
        "open_lien_count":    lien_cnt,
        "equity_estimate":    equity,
        "equity_fmt":         f"${equity:,.0f}" if equity else None,
        "ltv_current":        ltv,
        "ltv_purchase":       ltv_purch,
        "lender_name":        lender,
        "financing_hist_count": fin_hist,
        "source":             "Realie",
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
