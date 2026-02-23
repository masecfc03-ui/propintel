"""
ATTOM Data Solutions API scraper
https://developer.attomdata.com

Provides: AVM, sold comps, mortgage/lien data, ownership history, pre-foreclosure
Free trial: 100 calls/month — set ATTOM_API_KEY env var

Endpoints used:
  /propertyapi/v1.0.0/property/detail          — full property detail + AVM
  /propertyapi/v1.0.0/sale/snapshot            — sold comps (radius search)
  /propertyapi/v1.0.0/attomavm/detail          — AVM with confidence + range
  /propertyapi/v1.0.0/property/expandedprofile — mortgage + lien + ownership
"""

import os
import logging
import time
import urllib.request
import urllib.parse
import json

log = logging.getLogger(__name__)

BASE_URL   = "https://api.gateway.attomdata.com"
API_KEY    = os.environ.get("ATTOM_API_KEY", "")
TIMEOUT    = 15
MAX_RETRY  = 2


def _get(endpoint: str, params: dict) -> dict:
    """Make a GET request to ATTOM API."""
    if not API_KEY:
        return {"error": "ATTOM_API_KEY not configured"}

    url = f"{BASE_URL}{endpoint}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "apikey": API_KEY,
        },
        method="GET"
    )
    for attempt in range(MAX_RETRY):
        try:
            with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
                body = resp.read().decode("utf-8", errors="ignore")
                return json.loads(body)
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", errors="ignore")
            log.warning("ATTOM HTTP %s on %s: %s", e.code, endpoint, body[:200])
            if e.code == 429:
                time.sleep(2 ** attempt)
                continue
            return {"error": f"HTTP {e.code}", "detail": body[:300]}
        except Exception as ex:
            log.warning("ATTOM error on %s: %s", endpoint, ex)
            if attempt < MAX_RETRY - 1:
                time.sleep(1)
                continue
            return {"error": str(ex)}
    return {"error": "Max retries exceeded"}


# ─── PUBLIC FUNCTIONS ─────────────────────────────────────────────────────────

def get_avm(address: str, zipcode: str = "") -> dict:
    """
    Get ATTOM AVM (automated valuation) for an address.
    Returns: { value, value_low, value_high, confidence_score, calc_date, ... }
    """
    params = {"address1": address}
    if zipcode:
        params["address2"] = zipcode

    data = _get("/propertyapi/v1.0.0/attomavm/detail", params)
    if data.get("error"):
        return data

    try:
        prop = data["property"][0]
        avm  = prop.get("avm", {})
        amount = avm.get("amount", {})
        return {
            "available":        True,
            "value":            amount.get("value"),
            "value_low":        amount.get("low"),
            "value_high":       amount.get("high"),
            "value_fmt":        f"${amount.get('value',0):,.0f}" if amount.get("value") else None,
            "range_fmt":        f"${amount.get('low',0):,.0f} – ${amount.get('high',0):,.0f}"
                                if amount.get("low") and amount.get("high") else None,
            "confidence_score": avm.get("scr"),
            "calc_date":        avm.get("eventDate"),
            "source":           "ATTOM AVM",
        }
    except (KeyError, IndexError, TypeError) as e:
        log.warning("ATTOM AVM parse error: %s | raw: %s", e, str(data)[:300])
        return {"available": False, "error": str(e)}


def get_mortgage_lien(address: str, zipcode: str = "") -> dict:
    """
    Get mortgage, open lien, and equity data for an address.
    Returns estimated equity, open loan amount, lender, loan type, etc.
    """
    params = {"address1": address, "proptype": "all"}
    if zipcode:
        params["address2"] = zipcode

    data = _get("/propertyapi/v1.0.0/property/expandedprofile", params)
    if data.get("error"):
        return data

    try:
        prop   = data["property"][0]
        mort   = prop.get("mortgage", {}) or {}
        lien   = prop.get("openLien", {}) or {}
        sale   = prop.get("sale", {}) or {}
        amount = mort.get("amount", {}) or {}
        lender = mort.get("lender", {}) or {}

        last_sale = sale.get("saleAmountData", {}) or {}
        recording = sale.get("saleTransDate") or sale.get("saleRecDate")

        # Estimate equity: AVM – open loan balance
        open_balance = lien.get("openLienTotalBalance")

        return {
            "available":          True,
            "loan_amount":        amount.get("loanAmount"),
            "loan_amount_fmt":    f"${amount.get('loanAmount',0):,.0f}" if amount.get("loanAmount") else None,
            "loan_type":          mort.get("loanTypeCode") or mort.get("loanTypeName"),
            "interest_rate":      mort.get("interestRate"),
            "lender_name":        lender.get("institutionName"),
            "maturity_date":      mort.get("maturityDate"),
            "open_lien_count":    lien.get("openLienCount"),
            "open_lien_balance":  open_balance,
            "open_lien_fmt":      f"${open_balance:,.0f}" if open_balance else None,
            "last_sale_price":    last_sale.get("saleAmt"),
            "last_sale_price_fmt":f"${last_sale.get('saleAmt',0):,.0f}" if last_sale.get("saleAmt") else None,
            "last_sale_date":     recording,
            "source":             "ATTOM",
        }
    except (KeyError, IndexError, TypeError) as e:
        log.warning("ATTOM mortgage parse error: %s | raw: %s", e, str(data)[:300])
        return {"available": False, "error": str(e)}


def get_sold_comps(address: str, zipcode: str = "",
                   radius_miles: float = 0.5,
                   months_back: int = 12,
                   max_results: int = 10) -> dict:
    """
    Get sold comparable properties within radius over the past N months.
    Returns list of comps with address, price, SF, beds/baths, sold date.

    months_back: 3 = 90d, 6 = 180d, 12 = 1yr
    """
    params = {
        "address1": address,
        "radius":   str(radius_miles),
        "pagesize": str(max_results),
        "orderby":  "saleAmt",  # sort by sale price
    }
    if zipcode:
        params["address2"] = zipcode

    data = _get("/propertyapi/v1.0.0/sale/snapshot", params)
    if data.get("error"):
        return {"available": False, "comps": [], "error": data["error"]}

    try:
        raw_props = data.get("property", []) or []
    except (KeyError, TypeError):
        return {"available": False, "comps": [], "error": "Unexpected response format"}

    comps = []
    from datetime import datetime, timedelta
    cutoff = datetime.now() - timedelta(days=months_back * 30)

    for prop in raw_props:
        try:
            sale     = prop.get("sale", {}) or {}
            address_ = prop.get("address", {}) or {}
            bldg     = prop.get("building", {}) or {}
            size     = bldg.get("size", {}) or {}
            rooms    = bldg.get("rooms", {}) or {}

            sale_data   = sale.get("saleAmountData", {}) or {}
            sale_amount = sale_data.get("saleAmt")
            sale_date   = sale.get("saleTransDate") or sale.get("saleRecDate")

            # Filter by date
            if sale_date:
                try:
                    sale_dt = datetime.strptime(sale_date[:10], "%Y-%m-%d")
                    if sale_dt < cutoff:
                        continue
                except ValueError:
                    pass

            if not sale_amount:
                continue

            bldg_sf = size.get("universalSize") or size.get("livingSize")
            price_per_sf = round(sale_amount / bldg_sf, 0) if bldg_sf and bldg_sf > 0 else None

            comps.append({
                "address":      address_.get("oneLine") or address_.get("line1", ""),
                "city":         address_.get("city", ""),
                "state":        address_.get("stateCode", ""),
                "zip":          address_.get("postal1", ""),
                "sale_amount":  sale_amount,
                "sale_fmt":     f"${sale_amount:,.0f}",
                "sale_date":    sale_date,
                "beds":         rooms.get("bedsCount"),
                "baths":        rooms.get("bathsFullCalc") or rooms.get("bathsTotal"),
                "building_sf":  bldg_sf,
                "sf_fmt":       f"{int(bldg_sf):,} SF" if bldg_sf else None,
                "price_per_sf": price_per_sf,
                "psf_fmt":      f"${price_per_sf:,.0f}/SF" if price_per_sf else None,
                "year_built":   bldg.get("construction", {}).get("yearBuilt"),
                "use_type":     prop.get("summary", {}).get("proptype"),
                "days_ago":     (datetime.now() - datetime.strptime(sale_date[:10], "%Y-%m-%d")).days
                                if sale_date else None,
            })
        except Exception as ex:
            log.debug("ATTOM comp parse skip: %s", ex)
            continue

    # Sort by sale date (most recent first)
    comps.sort(key=lambda x: x.get("sale_date") or "", reverse=True)

    # Compute comp stats
    prices    = [c["sale_amount"] for c in comps if c["sale_amount"]]
    psf_vals  = [c["price_per_sf"] for c in comps if c.get("price_per_sf")]

    stats = {}
    if prices:
        stats["comp_count"]    = len(prices)
        stats["median_price"]  = sorted(prices)[len(prices)//2]
        stats["avg_price"]     = round(sum(prices) / len(prices), 0)
        stats["low_price"]     = min(prices)
        stats["high_price"]    = max(prices)
        stats["median_price_fmt"] = f"${stats['median_price']:,.0f}"
        stats["avg_price_fmt"]    = f"${stats['avg_price']:,.0f}"
        stats["price_range_fmt"]  = f"${stats['low_price']:,.0f} – ${stats['high_price']:,.0f}"
    if psf_vals:
        stats["median_psf"]    = round(sorted(psf_vals)[len(psf_vals)//2], 0)
        stats["median_psf_fmt"] = f"${stats['median_psf']:,.0f}/SF"

    return {
        "available":   len(comps) > 0,
        "comps":       comps,
        "stats":       stats,
        "radius_miles": radius_miles,
        "months_back": months_back,
        "source":      "ATTOM",
    }


def get_ownership_history(address: str, zipcode: str = "") -> dict:
    """
    Get deed/sale history — who bought it, when, for how much.
    Returns list of historical transactions sorted newest first.
    """
    params = {"address1": address}
    if zipcode:
        params["address2"] = zipcode

    data = _get("/propertyapi/v1.0.0/saleshistory/detail", params)
    if data.get("error"):
        return {"available": False, "history": [], "error": data["error"]}

    try:
        prop     = data["property"][0]
        raw_hist = prop.get("salehistory", []) or []
    except (KeyError, IndexError, TypeError):
        return {"available": False, "history": []}

    history = []
    for h in raw_hist:
        sale_data = h.get("saleAmountData", {}) or {}
        buyer     = h.get("buyer", [{}])
        buyer_name = buyer[0].get("fullName") if buyer else None

        history.append({
            "sale_date":     h.get("saleTransDate") or h.get("saleRecDate"),
            "sale_amount":   sale_data.get("saleAmt"),
            "sale_fmt":      f"${sale_data.get('saleAmt',0):,.0f}" if sale_data.get("saleAmt") else None,
            "buyer_name":    buyer_name,
            "document_type": h.get("document", {}).get("typeDescription") if h.get("document") else None,
        })

    history.sort(key=lambda x: x.get("sale_date") or "", reverse=True)

    # Compute hold duration from last purchase to today
    hold_years = None
    if history and history[0].get("sale_date"):
        from datetime import datetime
        try:
            bought = datetime.strptime(history[0]["sale_date"][:10], "%Y-%m-%d")
            hold_years = round((datetime.now() - bought).days / 365.25, 1)
        except ValueError:
            pass

    return {
        "available":    len(history) > 0,
        "history":      history,
        "hold_years":   hold_years,
        "source":       "ATTOM",
    }
