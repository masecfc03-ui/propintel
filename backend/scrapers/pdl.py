"""
People Data Labs (PDL) — Person Enrichment Skip Trace
Endpoint: GET /v5/person/enrich
Input: name + address from DCAD parcel record
Output: phones, emails, LinkedIn profile

Pricing: ~$0.10/successful match (charged only on 200 status)
API docs: https://docs.peopledatalabs.com/docs/person-enrichment-api

Best for: individual property owners
Limited on: LLCs/entities (falls back gracefully)
"""
import os
import re
import requests

PDL_API_KEY = os.environ.get("PDL_API_KEY", "")
PDL_URL = "https://api.peopledatalabs.com/v5/person/enrich"


def skip_trace(first_name: str, last_name: str,
               address: str = "", city: str = "",
               state: str = "TX", zip_code: str = "") -> dict:
    """
    Look up an individual owner via PDL.

    Returns:
        {
          "status": "hit" | "no_hit" | "error" | "disabled",
          "phones": [...],
          "emails": [...],
          "linkedin": "...",
          "source": "People Data Labs",
          "credits_used": 0 or 1,
        }
    """
    if not PDL_API_KEY:
        return _no_key()

    if not (first_name or last_name):
        return _no_hit("No individual name to search")

    full_name = (first_name + " " + last_name).strip()

    # Build location string
    location_parts = [p for p in [address, city, state, zip_code] if p]
    location = ", ".join(location_parts)

    params = {
        "name": full_name,
        "pretty": "false",
        "min_likelihood": "0.6",  # Only return high-confidence matches
    }
    if zip_code:
        params["postal_code"] = zip_code
    elif location:
        params["location"] = location

    headers = {"X-Api-Key": PDL_API_KEY}

    try:
        resp = requests.get(PDL_URL, params=params, headers=headers, timeout=15)

        if resp.status_code == 404:
            return _no_hit("No matching profile found")

        if resp.status_code == 402:
            return _error("PDL credit limit reached — check your plan")

        if resp.status_code == 401:
            return _error("Invalid PDL API key")

        if resp.status_code == 400:
            return _no_hit("Insufficient data for PDL lookup")

        if resp.status_code != 200:
            return _error(f"PDL returned {resp.status_code}")

        data = resp.json()
        person = data.get("data") or {}

        phones = _extract_phones(person)
        emails = _extract_emails(person)
        linkedin = person.get("linkedin_url", "")

        if not phones and not emails:
            return _no_hit("Profile found but no contact data available")

        return {
            "status": "hit",
            "phones": phones,
            "emails": emails,
            "dnc": [False] * len(phones),  # PDL does not provide DNC status
            "linkedin": linkedin,
            "full_name": person.get("full_name", full_name),
            "source": "People Data Labs",
            "credits_used": 1,
            "note": "Live skip trace via People Data Labs enrichment API.",
        }

    except requests.exceptions.Timeout:
        return _error("PDL request timed out")
    except Exception as e:
        return _error(str(e) or "PDL lookup failed")


def skip_trace_entity(entity_name: str, mailing_address: str = "",
                       mailing_city: str = "", mailing_state: str = "TX",
                       mailing_zip: str = "") -> dict:
    """
    PDL is not designed for entity/LLC lookups — returns graceful fallback.
    Caller should use TX SOS data instead.
    """
    return {
        "status": "entity",
        "phones": [],
        "emails": [],
        "source": "People Data Labs",
        "credits_used": 0,
        "note": (
            f"'{entity_name}' appears to be an entity — PDL covers individuals only. "
            "Contact info not available via skip trace. "
            "Try TX SOS registered agent or direct corporate lookup."
        ),
    }


def _extract_phones(person: dict) -> list:
    phones = []
    for p in (person.get("phone_numbers") or []):
        raw = str(p).replace("+1", "").replace("-", "").replace(" ", "").replace("(", "").replace(")", "")
        if len(raw) >= 10:
            phones.append(raw[-10:])  # normalize to 10 digits
    return phones[:3]  # return max 3 numbers


def _extract_emails(person: dict) -> list:
    emails = []
    for e in (person.get("emails") or []):
        if isinstance(e, dict):
            addr = e.get("address", "")
        else:
            addr = str(e)
        if addr and "@" in addr and not addr.endswith("example.com"):
            emails.append(addr)
    return emails[:3]


def _no_key():
    return {
        "status": "disabled",
        "phones": [],
        "emails": [],
        "source": "People Data Labs",
        "credits_used": 0,
        "note": "PDL_API_KEY not configured. Set it in backend/.env",
    }


def _no_hit(reason: str):
    return {
        "status": "no_hit",
        "phones": [],
        "emails": [],
        "source": "People Data Labs",
        "credits_used": 0,
        "note": reason,
    }


def _error(reason: str):
    return {
        "status": "error",
        "phones": [],
        "emails": [],
        "source": "People Data Labs",
        "credits_used": 0,
        "note": reason,
    }
