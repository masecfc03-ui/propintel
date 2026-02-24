"""
Permits scraper — pulls building permit history from city open data portals.
All sources are free, no API key required (Socrata open data).

Supported cities: Dallas, Houston, Austin, San Antonio (Texas)

Returns:
{
  "available": True/False,
  "city": str,
  "permits": [
    {
      "permit_number": str,
      "type": str,           # "Building", "Electrical", "Plumbing", "HVAC", etc.
      "description": str,
      "status": str,         # "Issued", "Expired", "Finaled", "Pending"
      "issued_date": str,    # YYYY-MM-DD
      "expiration_date": str,
      "value": float or None,  # dollar value of the work
      "contractor": str or None,
      "source": str          # "dallas_opendata", "houston_opendata", etc.
    }
  ],
  "summary": {
    "total": int,
    "unpermitted_risk": bool,  # True if no permits found for an established property
    "last_permit_date": str,
    "total_permit_value": float
  }
}
"""

import json
import re
import urllib.parse
import urllib.request
from datetime import datetime, date
from typing import Optional


# ── City routing ──────────────────────────────────────────────────────────────

CITY_PATTERNS = {
    "dallas":      ["dallas"],
    "houston":     ["houston"],
    "austin":      ["austin"],
    "san antonio": ["san antonio", "san_antonio"],
}

SOCRATA_TIMEOUT = 8  # seconds


def _detect_city(address: str, geo: Optional[dict] = None) -> Optional[str]:
    """
    Detect city from address string or geo dict.
    Returns lowercase normalized city key, or None if not supported.
    """
    # Prefer geocoded city (more reliable)
    city_raw = ""
    if geo and isinstance(geo, dict):
        city_raw = (geo.get("city") or "").lower().strip()

    # Fallback: parse from address string
    if not city_raw:
        city_raw = address.lower()

    for key, patterns in CITY_PATTERNS.items():
        for pat in patterns:
            if pat in city_raw:
                return key

    return None


def _socrata_fetch(base_url: str, where_clause: str, limit: int = 50) -> list:
    """
    Fetch records from a Socrata open data API endpoint.
    Uses $where SoQL filter and returns list of record dicts.
    Raises on network/timeout errors.
    """
    params = urllib.parse.urlencode({
        "$where": where_clause,
        "$limit": limit,
        "$order": "issued_date DESC" if "issued_date" in where_clause else ":id DESC",
    })
    url = f"{base_url}?{params}"
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=SOCRATA_TIMEOUT) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _street_number_name(address: str) -> tuple:
    """
    Extract street number and street name from a full address string.
    Returns (number_str, street_name_str) — both uppercased.
    """
    # Strip city/state/zip: take only the first component before comma
    street_part = address.split(",")[0].strip().upper()
    # Match leading digits (street number)
    m = re.match(r"^(\d+)\s+(.*)", street_part)
    if m:
        return m.group(1), m.group(2).strip()
    return "", street_part


def _parse_date(raw: str) -> str:
    """Normalize an ISO or common date string to YYYY-MM-DD, or return raw."""
    if not raw:
        return ""
    try:
        # Handle ISO 8601 with time component: "2021-03-15T00:00:00.000"
        return raw[:10]
    except Exception:
        return str(raw)


def _parse_value(raw) -> Optional[float]:
    """Parse a permit dollar value from string or number."""
    if raw is None:
        return None
    try:
        return float(str(raw).replace(",", "").replace("$", "").strip())
    except Exception:
        return None


# ── Dallas ────────────────────────────────────────────────────────────────────

DALLAS_URL = "https://www.dallasopendata.com/resource/46vm-7bqm.json"


def _get_dallas(address: str) -> list:
    """
    Fetch building permits from Dallas Open Data (Socrata).
    Dataset: Building Permits (46vm-7bqm)
    """
    _, street_name = _street_number_name(address)
    number, _ = _street_number_name(address)

    # Search by street name fragment (most reliable)
    keyword = street_name.split()[0] if street_name else ""
    if not keyword:
        return []

    # Try number + first street word combo
    where = f"upper(address) LIKE '%{number} {keyword}%'" if number else f"upper(address) LIKE '%{keyword}%'"

    try:
        raw = _socrata_fetch(DALLAS_URL, where, limit=50)
    except Exception:
        # Retry with broader match
        try:
            where2 = f"upper(address) LIKE '%{keyword}%'"
            raw = _socrata_fetch(DALLAS_URL, where2, limit=50)
        except Exception:
            return []

    permits = []
    for r in raw:
        addr_field = (r.get("address") or "").upper()
        # Filter: address must contain our street number
        if number and number not in addr_field:
            continue

        permit_type = _normalize_type(
            r.get("permit_type") or r.get("worktype") or r.get("work_type") or ""
        )
        permits.append({
            "permit_number": r.get("permit_num") or r.get("permit_number") or r.get("permitnum") or "",
            "type": permit_type,
            "description": (r.get("work_description") or r.get("description") or r.get("workdesc") or "")[:200],
            "status": _normalize_status(r.get("status") or r.get("permit_status") or ""),
            "issued_date": _parse_date(r.get("issue_date") or r.get("issued_date") or ""),
            "expiration_date": _parse_date(r.get("expiration_date") or r.get("exp_date") or ""),
            "value": _parse_value(r.get("project_value") or r.get("declared_value") or r.get("value")),
            "contractor": (r.get("contractor") or r.get("contractor_name") or r.get("contractor_trade") or None),
            "source": "dallas_opendata",
            "raw_address": r.get("address", ""),
        })

    return permits


# ── Houston ───────────────────────────────────────────────────────────────────

HOUSTON_URL = "https://data.houstontx.gov/resource/yqhd-c7vd.json"


def _get_houston(address: str) -> list:
    """
    Fetch building permits from Houston Open Data (Socrata).
    Dataset: Building Permits (yqhd-c7vd)
    """
    number, street_name = _street_number_name(address)
    keyword = street_name.split()[0] if street_name else ""
    if not keyword:
        return []

    where = f"upper(address) LIKE '%{number} {keyword}%'" if number else f"upper(address) LIKE '%{keyword}%'"

    try:
        raw = _socrata_fetch(HOUSTON_URL, where, limit=50)
    except Exception:
        try:
            where2 = f"upper(address) LIKE '%{keyword}%'"
            raw = _socrata_fetch(HOUSTON_URL, where2, limit=50)
        except Exception:
            return []

    permits = []
    for r in raw:
        addr_field = (
            r.get("address") or r.get("site_address") or r.get("streetnumber", "") + " " + r.get("streetname", "")
        ).upper()
        if number and number not in addr_field:
            continue

        permit_type = _normalize_type(
            r.get("worktype") or r.get("permit_type") or r.get("type_of_work") or ""
        )
        permits.append({
            "permit_number": r.get("permit_number") or r.get("permit_num") or r.get("permitnumber") or "",
            "type": permit_type,
            "description": (r.get("description") or r.get("work_description") or "")[:200],
            "status": _normalize_status(r.get("status") or r.get("permit_status") or ""),
            "issued_date": _parse_date(r.get("issued_date") or r.get("issue_date") or ""),
            "expiration_date": _parse_date(r.get("expiration_date") or r.get("exp_date") or ""),
            "value": _parse_value(r.get("declared_value") or r.get("project_value") or r.get("value")),
            "contractor": (r.get("contractor_name") or r.get("contractor") or None),
            "source": "houston_opendata",
            "raw_address": addr_field,
        })

    return permits


# ── Austin ────────────────────────────────────────────────────────────────────

AUSTIN_URL = "https://data.austintexas.gov/resource/3syk-w9eu.json"


def _get_austin(address: str) -> list:
    """
    Fetch building permits from Austin Open Data (Socrata).
    Dataset: Issued Construction Permits (3syk-w9eu)
    """
    number, street_name = _street_number_name(address)
    keyword = street_name.split()[0] if street_name else ""
    if not keyword:
        return []

    # Austin uses original_address1 field
    where = (
        f"upper(original_address1) LIKE '%{number} {keyword}%'"
        if number
        else f"upper(original_address1) LIKE '%{keyword}%'"
    )

    try:
        raw = _socrata_fetch(AUSTIN_URL, where, limit=50)
    except Exception:
        try:
            where2 = f"upper(original_address1) LIKE '%{keyword}%'"
            raw = _socrata_fetch(AUSTIN_URL, where2, limit=50)
        except Exception:
            return []

    permits = []
    for r in raw:
        addr_field = (r.get("original_address1") or r.get("address") or "").upper()
        if number and number not in addr_field:
            continue

        permit_type = _normalize_type(
            r.get("permit_type_desc") or r.get("worktype") or r.get("permit_class_mapped") or ""
        )
        permits.append({
            "permit_number": r.get("permit_num") or r.get("permitnum") or r.get("permit_number") or "",
            "type": permit_type,
            "description": (r.get("work_description") or r.get("description") or r.get("description_of_work") or "")[:200],
            "status": _normalize_status(r.get("status_current") or r.get("status") or ""),
            "issued_date": _parse_date(r.get("issued_date") or r.get("issue_date") or ""),
            "expiration_date": _parse_date(r.get("expiration_date") or r.get("expires_date") or ""),
            "value": _parse_value(r.get("total_job_valuation") or r.get("declared_value") or r.get("value")),
            "contractor": (r.get("contractor_company_name") or r.get("contractor") or None),
            "source": "austin_opendata",
            "raw_address": addr_field,
        })

    return permits


# ── San Antonio ───────────────────────────────────────────────────────────────

SAN_ANTONIO_URL = "https://data.sanantonio.gov/resource/keg4-n9ia.json"


def _get_san_antonio(address: str) -> list:
    """
    Fetch building permits from San Antonio Open Data (Socrata).
    Dataset: Building Permits (keg4-n9ia)
    """
    number, street_name = _street_number_name(address)
    keyword = street_name.split()[0] if street_name else ""
    if not keyword:
        return []

    # Try multiple common address field names
    where = (
        f"upper(address) LIKE '%{number} {keyword}%'"
        if number
        else f"upper(address) LIKE '%{keyword}%'"
    )

    try:
        raw = _socrata_fetch(SAN_ANTONIO_URL, where, limit=50)
    except Exception:
        try:
            # Fallback: try site_address field
            where2 = (
                f"upper(site_address) LIKE '%{number} {keyword}%'"
                if number
                else f"upper(site_address) LIKE '%{keyword}%'"
            )
            raw = _socrata_fetch(SAN_ANTONIO_URL, where2, limit=50)
        except Exception:
            return []

    permits = []
    for r in raw:
        addr_field = (r.get("address") or r.get("site_address") or r.get("location") or "").upper()
        if number and number not in addr_field:
            continue

        permit_type = _normalize_type(
            r.get("permit_type") or r.get("worktype") or r.get("type") or ""
        )
        permits.append({
            "permit_number": r.get("permit_number") or r.get("permit_num") or r.get("case_number") or "",
            "type": permit_type,
            "description": (r.get("description") or r.get("work_description") or r.get("project_description") or "")[:200],
            "status": _normalize_status(r.get("status") or r.get("permit_status") or ""),
            "issued_date": _parse_date(r.get("issued_date") or r.get("issue_date") or r.get("applied_date") or ""),
            "expiration_date": _parse_date(r.get("expiration_date") or r.get("exp_date") or ""),
            "value": _parse_value(r.get("value") or r.get("project_value") or r.get("declared_value")),
            "contractor": (r.get("contractor_name") or r.get("contractor") or None),
            "source": "san_antonio_opendata",
            "raw_address": addr_field,
        })

    return permits


# ── Normalization helpers ─────────────────────────────────────────────────────

_TYPE_MAP = {
    "building":   "Building",
    "electrical": "Electrical",
    "electric":   "Electrical",
    "plumbing":   "Plumbing",
    "mechanical": "Mechanical",
    "hvac":       "HVAC",
    "roofing":    "Roofing",
    "roof":       "Roofing",
    "demolition": "Demolition",
    "demo":       "Demolition",
    "fence":      "Fence",
    "pool":       "Pool",
    "sign":       "Sign",
    "fire":       "Fire",
    "sprinkler":  "Fire",
}


def _normalize_type(raw: str) -> str:
    low = raw.lower()
    for key, label in _TYPE_MAP.items():
        if key in low:
            return label
    return raw.title() if raw else "Other"


_STATUS_MAP = {
    "issued":    "Issued",
    "finaled":   "Finaled",
    "final":     "Finaled",
    "expired":   "Expired",
    "pending":   "Pending",
    "approved":  "Issued",
    "complete":  "Finaled",
    "completed": "Finaled",
    "active":    "Issued",
    "void":      "Voided",
    "cancelled": "Cancelled",
    "canceled":  "Cancelled",
    "withdrawn": "Withdrawn",
    "revoked":   "Voided",
}


def _normalize_status(raw: str) -> str:
    low = raw.lower()
    for key, label in _STATUS_MAP.items():
        if key in low:
            return label
    return raw.title() if raw else "Unknown"


# ── Summary calculation ───────────────────────────────────────────────────────

def _build_summary(permits: list, year_built: Optional[str] = None) -> dict:
    """
    Calculate summary stats and flag unpermitted work risk.

    unpermitted_risk: True when the property is established (year_built < 10 years ago)
    but has zero permits — suggests renovations/repairs done without permits.
    """
    total = len(permits)
    total_value = sum(p.get("value") or 0.0 for p in permits if p.get("value"))

    # Last permit date
    dates = [p["issued_date"] for p in permits if p.get("issued_date")]
    last_date = max(dates) if dates else None

    # Unpermitted risk heuristic:
    # - No permits found at all, AND
    # - Property is older than 5 years (so some maintenance is expected)
    current_year = date.today().year
    unpermitted_risk = False
    if total == 0:
        if year_built:
            try:
                built_year = int(str(year_built)[:4])
                if current_year - built_year > 5:
                    unpermitted_risk = True
            except Exception:
                unpermitted_risk = True  # Unknown build year + no permits = flag it
        else:
            unpermitted_risk = True  # No year info = conservative flag

    return {
        "total": total,
        "unpermitted_risk": unpermitted_risk,
        "last_permit_date": last_date or "",
        "total_permit_value": round(total_value, 2),
    }


# ── Public API ────────────────────────────────────────────────────────────────

CITY_SCRAPERS = {
    "dallas":      _get_dallas,
    "houston":     _get_houston,
    "austin":      _get_austin,
    "san antonio": _get_san_antonio,
}


def get_permits(address: str, geo: Optional[dict] = None) -> dict:
    """
    Main entry point. Detects city and fetches permit data from the appropriate
    open data portal.

    Args:
        address: Full property address string
        geo:     Optional geocode result dict (used to reliably detect city)

    Returns:
        Normalized permit dict (see module docstring for schema)
    """
    try:
        city_key = _detect_city(address, geo)

        if not city_key:
            supported = ", ".join(k.title() for k in CITY_SCRAPERS.keys())
            return {
                "available": False,
                "city": None,
                "error": f"Permit data only available for: {supported}",
                "permits": [],
                "summary": {"total": 0, "unpermitted_risk": False, "last_permit_date": "", "total_permit_value": 0.0},
            }

        scraper_fn = CITY_SCRAPERS[city_key]
        permits = scraper_fn(address)

        # Get year_built from geo/parcel if available
        year_built = None
        if isinstance(geo, dict):
            year_built = geo.get("year_built")

        summary = _build_summary(permits, year_built)

        # Remove internal raw_address field from output
        clean_permits = []
        for p in permits:
            p_clean = {k: v for k, v in p.items() if k != "raw_address"}
            clean_permits.append(p_clean)

        return {
            "available": True,
            "city": city_key.title(),
            "permits": clean_permits,
            "summary": summary,
        }

    except Exception as e:
        return {
            "available": False,
            "city": None,
            "error": str(e),
            "permits": [],
            "summary": {"total": 0, "unpermitted_risk": False, "last_permit_date": "", "total_permit_value": 0.0},
        }
