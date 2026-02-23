"""
FEMA Flood Zone lookup via National Flood Hazard Layer (NFHL) REST API.
Free. No API key. Real FEMA data.
Source: https://hazards.fema.gov/gis/nfhl/rest/services/public/NFHL/MapServer
"""
import requests

FEMA_URL = (
    "https://hazards.fema.gov/arcgis/rest/services/public/NFHL/"
    "MapServer/28/query"
)

ZONE_DESCRIPTIONS = {
    "A":   "High Flood Risk — 1% annual chance flood (no BFE determined)",
    "AE":  "High Flood Risk — 1% annual chance flood (BFE determined)",
    "AH":  "High Flood Risk — Shallow flooding, ponding (BFE determined)",
    "AO":  "High Flood Risk — Shallow flooding, alluvial fan",
    "VE":  "High Flood Risk — Coastal with wave action (BFE determined)",
    "X":   "Minimal Flood Hazard — Outside 500-year floodplain (no flood insurance required)",
    "X500":"Moderate Flood Hazard — 0.2% annual chance flood (500-year floodplain)",
    "D":   "Undetermined flood hazard",
    "NP":  "Not participating in NFIP",
}

def get_flood_zone(lat: float, lng: float) -> dict:
    """
    Returns flood zone info for a lat/lng coordinate.
    {
      "zone": "X",
      "description": "Minimal Flood Hazard — ...",
      "firm_panel": "48113C0285J",
      "effective_date": "2009-09-25",
      "source": "FEMA NFHL REST API"
    }
    """
    try:
        # Build URL manually — FEMA ArcGIS rejects URL-encoded commas in geometry param
        url = (
            f"{FEMA_URL}?geometry={lng},{lat}"
            f"&geometryType=esriGeometryPoint&inSR=4326"
            f"&spatialRel=esriSpatialRelIntersects"
            f"&outFields=FLD_ZONE,DFIRM_ID"
            f"&returnGeometry=false&f=json"
        )
        resp = requests.get(url, timeout=12)
        resp.raise_for_status()
        data = resp.json()

        features = data.get("features", [])
        if not features:
            return {
                "zone": "Unknown",
                "description": "No flood zone data returned for this location",
                "source": "FEMA NFHL REST API"
            }

        attrs = features[0].get("attributes", {})
        zone = attrs.get("FLD_ZONE", "Unknown")
        firm = attrs.get("DFIRM_ID", "N/A")

        return {
            "zone": zone,
            "description": ZONE_DESCRIPTIONS.get(zone, f"Flood zone {zone}"),
            "firm_panel": firm,
            "flood_insurance_required": zone not in ("X", "D"),
            "source": "FEMA National Flood Hazard Layer (NFHL) REST API",
            "source_url": "https://msc.fema.gov/portal/home",
        }

    except Exception as e:
        return {"error": str(e), "zone": "Lookup failed", "source": "FEMA NFHL"}
