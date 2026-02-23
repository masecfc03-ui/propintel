"""
Dallas Central Appraisal District (DCAD) — Property lookup by address or APN.
Source: https://www.dcad.org/property-search/
Public record. No authentication required.
"""
import requests
from bs4 import BeautifulSoup
import re
import time

BASE = "https://www.dcad.org"
SEARCH_URL = f"{BASE}/property-search/"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer": BASE,
}

def _clean(val: str) -> str:
    return " ".join(val.split()).strip() if val else ""

def search_by_address(address: str) -> dict:
    """
    Search DCAD by property address.
    Returns parcel data dict or error.
    """
    session = requests.Session()
    session.headers.update(HEADERS)

    try:
        # Step 1: Get search page (may need session/CSRF token)
        resp = session.get(SEARCH_URL, timeout=15)
        resp.raise_for_status()
        time.sleep(1)

        # Step 2: Submit search form
        # DCAD uses a POST or query param — inspect actual form action
        search_resp = session.get(
            SEARCH_URL,
            params={"situs": address, "searchType": "situs"},
            timeout=15
        )
        search_resp.raise_for_status()
        soup = BeautifulSoup(search_resp.text, "lxml")

        return _parse_results(soup, address)

    except requests.exceptions.Timeout:
        return {"error": "DCAD request timed out", "source": "DCAD"}
    except requests.exceptions.ConnectionError:
        return {"error": "DCAD connection failed", "source": "DCAD"}
    except Exception as e:
        return {"error": str(e), "source": "DCAD"}

def search_by_apn(apn: str) -> dict:
    """Search DCAD by account number (APN)."""
    session = requests.Session()
    session.headers.update(HEADERS)

    try:
        resp = session.get(
            SEARCH_URL,
            params={"account": apn, "searchType": "account"},
            timeout=15
        )
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")
        return _parse_results(soup, apn)

    except Exception as e:
        return {"error": str(e), "source": "DCAD"}

def _parse_results(soup: BeautifulSoup, query: str) -> dict:
    """
    Parse DCAD search results page.
    DCAD renders property data in tables — extract key fields.
    """
    result = {
        "source": "Dallas Central Appraisal District",
        "source_url": SEARCH_URL,
        "query": query,
    }

    # Try to find property data tables
    tables = soup.find_all("table")
    all_text = soup.get_text(" ", strip=True)

    # Extract account number / APN
    apn_match = re.search(r"Account[:\s#]+([0-9]{14,20})", all_text)
    if apn_match:
        result["apn"] = apn_match.group(1)

    # Owner name
    owner_match = re.search(r"Owner[:\s]+([A-Z][A-Z\s&,./'-]{2,60}?)(?:\s{2,}|ZIP|Addr)", all_text)
    if owner_match:
        result["owner_name"] = _clean(owner_match.group(1))

    # Mailing address
    mail_match = re.search(r"Mailing[:\s]+(.{10,80}?(?:TX|OK|LA|NM|AR)\s+\d{5})", all_text, re.IGNORECASE)
    if mail_match:
        result["owner_mailing"] = _clean(mail_match.group(1))

    # Legal description
    legal_match = re.search(r"Legal\s+Description[:\s]+(.{10,120}?)(?:\s{2,}|Owner|Account)", all_text, re.IGNORECASE)
    if legal_match:
        result["legal_description"] = _clean(legal_match.group(1))

    # Year built
    yr_match = re.search(r"Year\s+Built[:\s]+(\d{4})", all_text, re.IGNORECASE)
    if yr_match:
        result["year_built"] = yr_match.group(1)

    # Building SF
    sf_match = re.search(r"(?:Building|Improvement)\s+(?:Area|Size|SF)[:\s]+([\d,]+)\s*(?:SF|Sq)", all_text, re.IGNORECASE)
    if sf_match:
        result["building_sf"] = sf_match.group(1).replace(",", "")

    # Assessed values
    for label, key in [
        ("Land", "assessed_land"),
        ("Improvement", "assessed_improvement"),
        ("Total", "assessed_total"),
        ("Taxable", "taxable_value"),
    ]:
        val_match = re.search(rf"{label}\s+(?:Value|Market)?[:\s]+\$([\d,]+)", all_text, re.IGNORECASE)
        if val_match:
            result[key] = int(val_match.group(1).replace(",", ""))

    # Check for delinquency
    if re.search(r"delinquent|past due|delinquency", all_text, re.IGNORECASE):
        result["tax_delinquent"] = True
    else:
        result["tax_delinquent"] = False

    # Table parsing fallback — extract all label:value pairs
    for table in tables:
        rows = table.find_all("tr")
        for row in rows:
            cells = row.find_all(["td", "th"])
            if len(cells) >= 2:
                label = _clean(cells[0].get_text())
                value = _clean(cells[1].get_text())
                if label and value and len(label) < 40:
                    snake = label.lower().replace(" ", "_").replace("/", "_")
                    result.setdefault(f"raw_{snake}", value)

    if not result.get("apn") and not result.get("owner_name"):
        result["warning"] = "DCAD parsing returned limited data — manual lookup recommended"
        result["manual_url"] = f"https://www.dcad.org/property-search/?situs={query}"

    return result
