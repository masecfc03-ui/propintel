"""
Parse a real estate listing URL and extract public data.
Handles: LoopNet, Crexi, Zillow, Realtor.com, plain addresses.
Only extracts what's visible without login.
"""
import re
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

def detect_source(url: str) -> str:
    domain = urlparse(url).netloc.lower()
    if "loopnet" in domain:
        return "loopnet"
    if "crexi" in domain:
        return "crexi"
    if "zillow" in domain:
        return "zillow"
    if "realtor.com" in domain:
        return "realtor"
    if "costar" in domain:
        return "costar"
    return "unknown"

def is_address(text: str) -> bool:
    """Detect if input is an address rather than a URL."""
    return not text.startswith("http") and bool(
        re.search(r"\d+\s+\w+\s+(st|ave|rd|ln|blvd|dr|way|ct|pl|cir|hwy)", text, re.IGNORECASE)
    )

def parse_listing(url: str) -> dict:
    """
    Fetch listing page and extract visible public data.
    Returns structured dict of listing facts.
    """
    source = detect_source(url)
    result = {"url": url, "source_site": source, "listing_id": _extract_id(url, source)}

    try:
        resp = requests.get(url, headers=HEADERS, timeout=15, allow_redirects=True)

        # LoopNet often 403s — handle gracefully
        if resp.status_code == 403:
            result["blocked"] = True
            result["note"] = f"{source.title()} blocked automated access. Extract listing ID from URL and enter details manually."
            result["listing_id"] = _extract_id(url, source)
            return result

        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")

        # Extract Open Graph / meta tags — most listing sites populate these
        og = {}
        for tag in soup.find_all("meta"):
            prop = tag.get("property", "") or tag.get("name", "")
            content = tag.get("content", "")
            if prop and content:
                og[prop] = content

        result["title"] = og.get("og:title") or soup.title.string if soup.title else ""
        result["description"] = og.get("og:description", "")
        result["image"] = og.get("og:image", "")

        # Try to extract structured data (JSON-LD)
        import json
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                ld = json.loads(script.string)
                if isinstance(ld, list):
                    ld = ld[0]
                if ld.get("@type") in ("RealEstateListing", "Offer", "Product"):
                    result["json_ld"] = ld
                    result["price"] = ld.get("price") or ld.get("offers", {}).get("price")
                    result["address_raw"] = ld.get("address", {})
            except Exception:
                pass

        # Source-specific parsers
        if source == "loopnet":
            result.update(_parse_loopnet(soup, og))
        elif source == "crexi":
            result.update(_parse_crexi(soup, og))
        elif source == "zillow":
            result.update(_parse_zillow(soup, og))
        elif source == "realtor":
            result.update(_parse_realtor(soup, og))

        # Generic fallback — find price patterns in page text
        if not result.get("asking_price"):
            text = soup.get_text()
            price_m = re.search(r"\$\s*([\d,]+(?:\.\d+)?)\s*(?:M|million)?", text)
            if price_m:
                raw = price_m.group(1).replace(",", "")
                result["asking_price_raw"] = f"${price_m.group(0).strip()}"

        result["fetch_status"] = "ok"

    except requests.exceptions.Timeout:
        result["error"] = "Request timed out"
    except Exception as e:
        result["error"] = str(e)

    return result

def _extract_id(url: str, source: str) -> str:
    """Extract listing ID from URL."""
    if source == "loopnet":
        m = re.search(r"/(\d{7,10})/?$", url)
        return m.group(1) if m else ""
    if source == "crexi":
        m = re.search(r"/properties/[\w-]+-(\d+)", url)
        return m.group(1) if m else ""
    return ""

def _clean_price(text: str):
    if not text:
        return None
    clean = re.sub(r"[^\d.]", "", text)
    try:
        val = float(clean)
        if val < 10000:  # probably in millions
            val *= 1_000_000
        return int(val)
    except Exception:
        return None

def _parse_loopnet(soup: BeautifulSoup, og: dict) -> dict:
    data = {}
    text = soup.get_text(" ")

    # Price
    price_m = re.search(r"\$\s*([\d,.]+)\s*(?:M(?:illion)?)?", text)
    if price_m:
        data["asking_price_raw"] = price_m.group(0).strip()

    # Cap rate
    cap_m = re.search(r"(?:Cap Rate|Capitalization)[:\s]+([\d.]+)%", text, re.IGNORECASE)
    if cap_m:
        data["cap_rate"] = cap_m.group(1) + "%"

    # SF
    sf_m = re.search(r"([\d,]+)\s*(?:SF|Sq\.?\s*Ft)", text, re.IGNORECASE)
    if sf_m:
        data["building_sf"] = sf_m.group(1).replace(",", "")

    # Property type
    type_m = re.search(r"Property Type[:\s]+([A-Za-z /]+?)(?:\n|\s{2,})", text, re.IGNORECASE)
    if type_m:
        data["property_type"] = type_m.group(1).strip()

    return data

def _parse_crexi(soup: BeautifulSoup, og: dict) -> dict:
    return {"note": "Crexi — extracted from public meta tags"}

def _parse_zillow(soup: BeautifulSoup, og: dict) -> dict:
    return {"note": "Zillow — extracted from public meta tags"}

def _parse_realtor(soup: BeautifulSoup, og: dict) -> dict:
    return {"note": "Realtor.com — extracted from public meta tags"}
