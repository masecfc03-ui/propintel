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


# Progressive radius + timeframe expansion for comps
_RADIUS_STEPS = [0.5, 1, 2, 5, 10, 20]
_TIME_STEPS   = [18, 36, 60]
_TARGET_COMPS = 5


def _parse_comp(prop):
    """Parse a single Realie comparable property dict. Returns None if invalid."""
    try:
        sale_price = prop.get("transferPrice")
        if not sale_price:
            return None

        # Always skip bad-data records
        if sale_price < 10_000:
            return None
        if sale_price > 5_000_000:
            return None

        # transferDate format: "20250514" → normalize to "2025-05-14"
        raw_date  = prop.get("transferDate") or prop.get("recordingDate")
        sale_date = None
        if raw_date and len(str(raw_date)) == 8:
            d = str(raw_date)
            sale_date = "{}-{}-{}".format(d[:4], d[4:6], d[6:])
        elif raw_date:
            sale_date = str(raw_date)[:10]

        bldg_sf      = prop.get("buildingArea")
        price_per_sf = round(sale_price / bldg_sf, 0) if bldg_sf and bldg_sf > 0 else None
        addr_str     = prop.get("addressFull") or prop.get("address", "")
        avm          = prop.get("modelValue")

        return {
            "address":      addr_str,
            "city":         prop.get("city", ""),
            "state":        prop.get("state", ""),
            "zip":          prop.get("zipCode", ""),
            "sale_amount":  sale_price,
            "sale_fmt":     "${:,.0f}".format(sale_price),
            "sale_date":    sale_date,
            "beds":         prop.get("totalBedrooms"),
            "baths":        prop.get("totalBathrooms"),
            "building_sf":  bldg_sf,
            "sf_fmt":       "{:,} SF".format(int(bldg_sf)) if bldg_sf else None,
            "price_per_sf": price_per_sf,
            "psf_fmt":      "${:,.0f}/SF".format(price_per_sf) if price_per_sf else None,
            "year_built":   prop.get("yearBuilt"),
            "acres":        prop.get("acres"),
            "avm":          avm,
            "avm_fmt":      "${:,.0f}".format(avm) if avm else None,
            "lien_balance": prop.get("totalLienBalance"),
            "equity_est":   prop.get("equityCurrentEstBal"),
            "owner_name":   prop.get("ownerName"),
            "owner_state":  prop.get("ownerState"),
            "lender_name":  prop.get("lenderName"),
            "subdivision":  prop.get("subdivision"),
            "owner_count":  prop.get("ownerParcelCount"),
            "_is_residential": prop.get("residential"),
        }
    except Exception as ex:
        log.debug("Realie comp parse skip: %s", ex)
        return None


def _filter_comps(raw_props, is_residential):
    """Filter and parse a list of raw comp dicts."""
    results = []
    for prop in raw_props:
        c = _parse_comp(prop)
        if c is None:
            continue
        # Residential-specific filters
        if is_residential:
            if c.get("_is_residential") is False:
                continue
            if c.get("building_sf") and c["building_sf"] > 20_000:
                continue
        results.append(c)
    results.sort(key=lambda x: x.get("sale_date") or "", reverse=True)
    return results


def _build_comps_result(comps, radius_used, months_back, max_results=10):
    """Build the standardized comps return dict from a filtered list."""
    limited = comps[:max_results]
    prices   = [c["sale_amount"] for c in limited if c["sale_amount"]]
    psf_vals = [c["price_per_sf"] for c in limited if c.get("price_per_sf")]

    stats = {}
    if prices:
        stats["comp_count"]       = len(prices)
        stats["median_price"]     = sorted(prices)[len(prices) // 2]
        stats["avg_price"]        = round(sum(prices) / len(prices), 0)
        stats["low_price"]        = min(prices)
        stats["high_price"]       = max(prices)
        stats["median_price_fmt"] = "${:,.0f}".format(stats["median_price"])
        stats["avg_price_fmt"]    = "${:,.0f}".format(stats["avg_price"])
        stats["price_range_fmt"]  = "${:,.0f} \u2013 ${:,.0f}".format(stats["low_price"], stats["high_price"])
    if psf_vals:
        stats["median_psf"]     = round(sorted(psf_vals)[len(psf_vals) // 2], 0)
        stats["median_psf_fmt"] = "${:,.0f}/SF".format(stats["median_psf"])

    return {
        "available":    len(limited) > 0,
        "comps":        limited,
        "stats":        stats,
        "radius_miles": radius_used,
        "radius_used":  radius_used,
        "months_back":  months_back,
        "source":       "Realie",
    }


def get_sold_comps(address=None, zipcode="",
                   lat=None, lng=None,
                   radius_miles=1.0,
                   months_back=18,
                   max_results=10,
                   property_type=None):
    """
    Get sold comparable properties using progressive radius + timeframe expansion.
    Guarantees at least _TARGET_COMPS (5) comps when data is available.
    Tries radii 0.5→1→2→5→10→20 mi and timeframes 18→36→60 months.

    Returns list of comps with address, price, SF, sold date, $/SF.
    Also returns radius_used and months_back so the report can display context.
    """
    # 1. Resolve lat/lng
    if not lat or not lng:
        try:
            from .geocode import geocode_address
            geo = geocode_address(address)
            lat = geo.get("lat")
            lng = geo.get("lng")
        except Exception:
            pass

    if not lat or not lng:
        detail = _address_lookup(address)
        if not detail.get("error"):
            lat = detail.get("latitude") or detail.get("lat")
            lng = detail.get("longitude") or detail.get("lng")

    if not lat or not lng:
        return {
            "available":   False,
            "comps":       [],
            "error":       "lat/lng required for Realie comps — geocoding failed",
            "radius_used": None,
            "months_back": None,
        }

    is_residential = (property_type or "").upper() in ("RESIDENTIAL", "MULTIFAMILY")

    best_comps  = []
    best_radius = _RADIUS_STEPS[-1]
    best_time   = _TIME_STEPS[-1]

    for time_frame in _TIME_STEPS:
        for radius in _RADIUS_STEPS:
            params = {
                "latitude":   round(lat, 6),
                "longitude":  round(lng, 6),
                "radius":     radius,
                "timeFrame":  time_frame,
                "maxResults": 50,   # fetch max from API; we filter + cap locally
            }

            resp = _get("public/premium/comparables/", params)
            if resp.get("error"):
                # API error — skip this combination but keep trying
                log.debug("Realie comps error at radius=%s mo=%s: %s", radius, time_frame, resp["error"])
                continue

            raw_props = (
                resp.get("comparables")
                or resp.get("properties")
                or (resp if isinstance(resp, list) else [])
            )

            filtered = _filter_comps(raw_props, is_residential)

            # Track best result seen so far
            if len(filtered) > len(best_comps):
                best_comps  = filtered
                best_radius = radius
                best_time   = time_frame

            if len(filtered) >= _TARGET_COMPS:
                log.info("Realie comps: %d found at radius=%smi mo=%s", len(filtered), radius, time_frame)
                return _build_comps_result(filtered, radius, time_frame, max_results)

    # Exhausted all combinations — return best we found
    if best_comps:
        log.info("Realie comps: only %d found after full expansion (best: %smi/%smo)",
                 len(best_comps), best_radius, best_time)
        return _build_comps_result(best_comps, best_radius, best_time, max_results)

    return {
        "available":   False,
        "comps":       [],
        "error":       "No comps found after progressive expansion (max 20mi / 60mo)",
        "radius_used": _RADIUS_STEPS[-1],
        "months_back": _TIME_STEPS[-1],
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
    orig_loan = raw.get("loanAmount") or raw.get("originalLoanAmount")
    loan_date = raw.get("loanOriginationDate") or raw.get("loanDate")

    if lien_bal is None and equity is None:
        return {"available": False, "error": "No lien/equity data for this property"}

    return {
        "available":              True,
        "open_lien_balance":      lien_bal,
        "open_lien_fmt":          f"${lien_bal:,.0f}" if lien_bal else None,
        "open_lien_count":        lien_cnt,
        "equity_estimate":        equity,
        "equity_fmt":             f"${equity:,.0f}" if equity else None,
        "ltv_current":            ltv,
        "ltv_purchase":           ltv_purch,
        "lender_name":            lender,
        "financing_hist_count":   fin_hist,
        "original_loan_amount":   orig_loan,
        "loan_origination_date":  loan_date,
        "source":                 "Realie",
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
