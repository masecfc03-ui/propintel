"""
DataZapp Skip Trace — Phone & Email Append API
Submits a single-record CSV, polls for completion, returns contact data.

API docs: https://knowledgebase.datazapp.com/apis/
Usage:    set DATAZAPP_API_KEY in .env

Cost:     ~$0.03/hit (phone), ~$0.03/hit (email)
"""
import os
import io
import csv
import time
import requests
from typing import Optional

DATAZAPP_API_KEY = os.environ.get("DATAZAPP_API_KEY", "")

# These endpoints are provided after account activation — update if needed
PHONE_SUBMIT_URL = "https://app.datazapp.com/api/PhoneAppend"
PHONE_STATUS_URL = "https://app.datazapp.com/api/PhoneAppendStatus"
EMAIL_SUBMIT_URL = "https://app.datazapp.com/api/EmailAppend"
EMAIL_STATUS_URL = "https://app.datazapp.com/api/EmailAppendStatus"

MAX_POLL_SECONDS = 60
POLL_INTERVAL = 3


def skip_trace(first_name: str, last_name: str, address: str,
               city: str = "", state: str = "TX", zip_code: str = "") -> dict:
    """
    Run phone + email skip trace on a property owner.

    Args:
        first_name: Owner's first name (parsed from DCAD)
        last_name:  Owner's last name (parsed from DCAD)
        address:    Owner's mailing address (from DCAD)
        city, state, zip_code: Mailing address components

    Returns:
        {
          "status": "hit" | "no_hit" | "error" | "disabled",
          "phones": ["9728347204", ...],
          "emails": ["owner@example.com", ...],
          "dnc": [True/False, ...],
          "source": "DataZapp",
          "credits_used": 1,
          "note": "..."
        }
    """
    if not DATAZAPP_API_KEY:
        return {
            "status": "disabled",
            "phones": [],
            "emails": [],
            "note": "DataZapp API key not configured. Set DATAZAPP_API_KEY in .env",
            "source": "DataZapp",
        }

    if not first_name and not last_name:
        return {
            "status": "error",
            "phones": [],
            "emails": [],
            "note": "Owner name required for skip trace",
            "source": "DataZapp",
        }

    phones = _phone_append(first_name, last_name, address, state, zip_code)
    emails = _email_append(first_name, last_name, address, state, zip_code)

    if phones.get("phones") or emails.get("emails"):
        return {
            "status": "hit",
            "phones": phones.get("phones", []),
            "emails": emails.get("emails", []),
            "dnc": phones.get("dnc", []),
            "source": "DataZapp",
            "credits_used": (1 if phones.get("phones") else 0) + (1 if emails.get("emails") else 0),
            "note": "Live skip trace — handle contact data with care.",
        }

    return {
        "status": "no_hit",
        "phones": [],
        "emails": [],
        "source": "DataZapp",
        "note": "No matching records found. Owner may be unlisted or record recently changed.",
    }


def skip_trace_entity(entity_name: str, mailing_address: str,
                       mailing_city: str = "", mailing_state: str = "TX",
                       mailing_zip: str = "") -> dict:
    """
    Skip trace an LLC/entity by looking up the registered agent.
    Falls back to address-only search for contact matching.
    """
    # Entity names don't have first/last — parse registered agent if available
    # For now, attempt trace on entity name split
    parts = entity_name.strip().split()
    if len(parts) >= 2:
        first = parts[0]
        last = " ".join(parts[1:])
    else:
        first = entity_name
        last = ""

    return skip_trace(
        first_name=first,
        last_name=last,
        address=mailing_address,
        city=mailing_city,
        state=mailing_state,
        zip_code=mailing_zip,
    )


def _build_csv_row(first: str, last: str, address: str, state: str, zip_code: str) -> bytes:
    """Build a single-row CSV in DataZapp's expected format."""
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["First", "Last", "Address", "State", "ZIP"])
    writer.writerow([
        (first or "").strip(),
        (last or "").strip(),
        (address or "").strip(),
        (state or "TX").strip().upper(),
        (zip_code or "").strip(),
    ])
    return buf.getvalue().encode("utf-8")


def _phone_append(first: str, last: str, address: str, state: str, zip_code: str) -> dict:
    """Submit phone append job and wait for result."""
    try:
        csv_data = _build_csv_row(first, last, address, state, zip_code)

        form = {
            "ApiKey": (None, DATAZAPP_API_KEY),
            "AppendModule": (None, "2"),
            "AppendType": (None, "3"),  # Cell (Priority) or Landline
            "IsDNC": (None, "1"),
            "fileName": (None, "skip_trace.csv"),
            "file": ("skip_trace.csv", csv_data, "text/csv"),
        }

        resp = requests.post(PHONE_SUBMIT_URL, files=form, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        token = (data.get("ResponseDetails") or {}).get("Token")
        if not token:
            return {"phones": [], "dnc": [], "error": "No token returned from phone append"}

        # Poll for completion
        result_url = _poll_for_result(PHONE_STATUS_URL, token, "2")
        if not result_url:
            return {"phones": [], "dnc": [], "error": "Phone append timed out"}

        # Download result CSV
        phones, dnc = _parse_phone_result(result_url)
        return {"phones": phones, "dnc": dnc}

    except Exception as e:
        return {"phones": [], "dnc": [], "error": str(e)}


def _email_append(first: str, last: str, address: str, state: str, zip_code: str) -> dict:
    """Submit email append job and wait for result."""
    try:
        csv_data = _build_csv_row(first, last, address, state, zip_code)

        form = {
            "ApiKey": (None, DATAZAPP_API_KEY),
            "AppendModule": (None, "1"),
            "fileName": (None, "skip_trace.csv"),
            "file": ("skip_trace.csv", csv_data, "text/csv"),
        }

        resp = requests.post(EMAIL_SUBMIT_URL, files=form, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        token = (data.get("ResponseDetails") or {}).get("Token")
        if not token:
            return {"emails": [], "error": "No token returned from email append"}

        result_url = _poll_for_result(EMAIL_STATUS_URL, token, "1")
        if not result_url:
            return {"emails": [], "error": "Email append timed out"}

        emails = _parse_email_result(result_url)
        return {"emails": emails}

    except Exception as e:
        return {"emails": [], "error": str(e)}


def _poll_for_result(status_url: str, token: str, module: str) -> Optional[str]:
    """Poll DataZapp status endpoint until job completes. Returns download URL or None."""
    start = time.time()
    while time.time() - start < MAX_POLL_SECONDS:
        try:
            form = {
                "ApiKey": (None, DATAZAPP_API_KEY),
                "Token": (None, token),
                "AppendModule": (None, module),
            }
            resp = requests.post(status_url, files=form, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            details = data.get("ResponseDetails") or {}
            status = str(details.get("Status", "")).lower()

            if status in ("true", "completed", "done", "1"):
                return details.get("DownloadUrl") or details.get("FileUrl")

            if status in ("false", "failed", "error"):
                return None

        except Exception:
            pass
        time.sleep(POLL_INTERVAL)
    return None


def _parse_phone_result(download_url: str):
    """Download result CSV and extract phone numbers."""
    phones = []
    dnc_flags = []
    try:
        resp = requests.get(download_url, timeout=20)
        resp.raise_for_status()
        reader = csv.DictReader(io.StringIO(resp.text))
        for row in reader:
            # Common column names DataZapp uses
            for key in ("Phone1", "CellPhone1", "Phone", "CellPhone", "phone"):
                val = (row.get(key) or "").strip()
                if val and len(val) >= 10:
                    phones.append(val)
                    dnc_flags.append(str(row.get("DNC", "0")).strip() == "1")
                    break
    except Exception:
        pass
    return phones, dnc_flags


def _parse_email_result(download_url: str):
    """Download result CSV and extract email addresses."""
    emails = []
    try:
        resp = requests.get(download_url, timeout=20)
        resp.raise_for_status()
        reader = csv.DictReader(io.StringIO(resp.text))
        for row in reader:
            for key in ("Email1", "Email", "EmailAddress", "email"):
                val = (row.get(key) or "").strip()
                if val and "@" in val:
                    emails.append(val)
                    break
    except Exception:
        pass
    return emails


def parse_owner_name(owner_name: str) -> tuple:
    """
    Parse owner name from DCAD into first/last for skip trace.
    DCAD returns names in LAST, FIRST format for individuals.
    LLCs/entities are returned as-is.

    Returns: (first_name, last_name, is_entity)
    """
    if not owner_name:
        return ("", "", False)

    # Check if it's an entity
    import re
    entity_keywords = r"\b(LLC|LP|LTD|INC|CORP|TRUST|ESTATE|PROPERTIES|HOLDINGS|GROUP|VENTURES|CO\b)"
    if re.search(entity_keywords, owner_name.upper()):
        return ("", owner_name, True)

    # Individual: try LAST, FIRST format
    if "," in owner_name:
        parts = owner_name.split(",", 1)
        last = parts[0].strip()
        first = parts[1].strip().split()[0] if parts[1].strip() else ""
        return (first, last, False)

    # Space-separated: FIRST LAST
    parts = owner_name.strip().split()
    if len(parts) >= 2:
        return (parts[0], parts[-1], False)
    return (owner_name, "", False)
