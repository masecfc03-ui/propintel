"""
Texas Secretary of State — Business search by address.
Identifies active/dissolved entities registered at a property address.
Source: https://www.sos.state.tx.us/corp/businesssearch.shtml
Public record.
"""
import requests
from bs4 import BeautifulSoup
import time

SEARCH_URL = "https://mycpa.cpa.state.tx.us/coa/Index.do"
SOS_URL = "https://www.sos.state.tx.us/corp/businesssearch.shtml"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
}

STATUS_FLAGS = {
    "active": "green",
    "in existence": "green",
    "forfeited": "red",
    "dissolved": "red",
    "withdrawn": "red",
    "revoked": "red",
    "default": "yellow",
}

def get_status_flag(status: str) -> str:
    s = status.lower()
    for key, flag in STATUS_FLAGS.items():
        if key in s:
            return flag
    return "yellow"

def search_by_address(street_address: str, city: str = "", state: str = "TX") -> list:
    """
    Search TX Comptroller (active franchise tax accounts) by address.
    Returns list of businesses:
    [
      {
        "name": "JADE NAILS & SPA LLC",
        "status": "Active",
        "status_flag": "green",
        "file_date": "04/2019",
        "source": "Texas Secretary of State"
      }
    ]
    """
    session = requests.Session()
    session.headers.update(HEADERS)
    results = []

    try:
        # TX Comptroller COA search by address
        resp = session.post(
            SEARCH_URL,
            data={
                "tpid": "",
                "taxType": "FRANCHISE",
                "name": "",
                "addr": street_address,
                "city": city,
                "state": state,
                "zip": "",
                "searchType": "address",
            },
            timeout=15
        )
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")
        time.sleep(0.5)

        # Parse results table
        table = soup.find("table", {"id": "coa-grid"}) or soup.find("table", class_="table")
        if table:
            rows = table.find_all("tr")[1:]  # skip header
            for row in rows:
                cells = row.find_all("td")
                if len(cells) >= 3:
                    name = cells[0].get_text(strip=True)
                    status = cells[1].get_text(strip=True) if len(cells) > 1 else "Unknown"
                    file_date = cells[2].get_text(strip=True) if len(cells) > 2 else ""

                    results.append({
                        "name": name,
                        "status": status,
                        "status_flag": get_status_flag(status),
                        "file_date": file_date,
                        "source": "Texas Comptroller of Public Accounts",
                        "source_url": SEARCH_URL,
                    })

    except Exception as e:
        results.append({
            "error": str(e),
            "note": "TX SOS/Comptroller scrape failed — manual lookup recommended",
            "manual_url": f"https://www.sos.state.tx.us/corp/businesssearch.shtml"
        })

    return results


def search_entity(entity_name: str) -> dict:
    """
    Look up a specific entity name on TX SOS to get LLC details,
    registered agent, and managing members.
    """
    session = requests.Session()
    session.headers.update(HEADERS)

    try:
        resp = session.get(
            SOS_URL,
            params={
                "action": "getCorpSearchURL",
                "nameTerm": entity_name,
                "nameType": "EXACT",
                "searchTerm": entity_name,
                "stype": "BNAM",
                "status": "A",
            },
            timeout=15
        )
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")

        result = {
            "entity_name": entity_name,
            "source": "Texas Secretary of State",
            "source_url": SOS_URL,
        }

        text = soup.get_text(" ", strip=True)

        import re
        status_match = re.search(r"Status[:\s]+([A-Za-z\s]+?)(?:\s{2,}|Date|File)", text)
        if status_match:
            result["sos_status"] = status_match.group(1).strip()

        date_match = re.search(r"(?:Formation|Filing)\s+Date[:\s]+([\d/]+)", text, re.IGNORECASE)
        if date_match:
            result["formation_date"] = date_match.group(1)

        agent_match = re.search(r"Registered\s+Agent[:\s]+([A-Z][A-Z\s,.-]{2,60}?)(?:\s{2,}|Addr|Office)", text)
        if agent_match:
            result["registered_agent"] = agent_match.group(1).strip()

        return result

    except Exception as e:
        return {
            "error": str(e),
            "entity_name": entity_name,
            "manual_url": f"https://www.sos.state.tx.us/corp/businesssearch.shtml"
        }
