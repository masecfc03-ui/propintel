"""
Geocode an address → lat, lng, county, state, ZIP
Uses the Census Geocoder — free, no API key required.
"""
import requests

GEOCODE_URL = "https://geocoding.geo.census.gov/geocoder/locations/onelineaddress"

def geocode(address: str) -> dict:
    """
    Returns:
    {
      "lat": 32.9141,
      "lng": -96.6389,
      "matched_address": "3229 FOREST LN, GARLAND, TX, 75042",
      "zip": "75042",
      "state": "TX",
      "county": "Dallas County",
      "county_fips": "48113"
    }
    """
    try:
        resp = requests.get(GEOCODE_URL, params={
            "address": address,
            "benchmark": "Public_AR_Current",
            "format": "json"
        }, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        matches = data.get("result", {}).get("addressMatches", [])
        if not matches:
            return {"error": "No geocode match found", "input": address}

        m = matches[0]
        coords = m.get("coordinates", {})
        geo = m.get("addressComponents", {})

        return {
            "lat": coords.get("y"),
            "lng": coords.get("x"),
            "matched_address": m.get("matchedAddress", address),
            "zip": geo.get("zip"),
            "state": geo.get("state"),
            "county": geo.get("county"),   # e.g. "Dallas County"
            "city": geo.get("city"),
        }

    except Exception as e:
        return {"error": str(e), "input": address}
