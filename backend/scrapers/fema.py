"""
FEMA Flood Zone lookup via National Flood Hazard Layer (NFHL) REST API.
Free. No API key. Real FEMA data.

Tries 3 endpoints in order — hazards.fema.gov times out on some cloud IPs,
Esri-hosted mirror is faster and more reliable on Render.
"""
import requests

# Primary: hazards.fema.gov direct
FEMA_PRIMARY = (
    "https://hazards.fema.gov/arcgis/rest/services/public/NFHL/"
    "MapServer/28/query"
)

# Mirror 1: Esri ArcGIS Online hosted (more reliable from cloud IPs)
FEMA_MIRROR_1 = (
    "https://hazards-fema.maps.arcgis.com/arcgis/rest/services/"
    "FIRMette/NFHLREST_FIRMette/MapServer/28/query"
)

# Mirror 2: MSC FEMA portal
FEMA_MIRROR_2 = (
    "https://msc.fema.gov/arcgis/rest/services/NFHL/NFHL/"
    "MapServer/28/query"
)

ZONE_DESCRIPTIONS = {
    "A":    "High Flood Risk — 1% annual chance flood (no BFE determined)",
    "AE":   "High Flood Risk — 1% annual chance flood (BFE determined)",
    "AH":   "High Flood Risk — Shallow flooding, ponding (BFE determined)",
    "AO":   "High Flood Risk — Shallow flooding, alluvial fan",
    "VE":   "High Flood Risk — Coastal with wave action (BFE determined)",
    "X":    "Minimal Flood Hazard — Outside 500-year floodplain (no flood insurance required)",
    "X500": "Moderate Flood Hazard — 0.2% annual chance flood (500-year floodplain)",
    "D":    "Undetermined flood hazard",
    "NP":   "Not participating in NFIP",
}


def _query_fema(base_url, lat, lng, timeout=20):
    """Query one FEMA endpoint. Returns features list or raises."""
    url = (
        f"{base_url}?geometry={lng},{lat}"
        f"&geometryType=esriGeometryPoint&inSR=4326"
        f"&spatialRel=esriSpatialRelIntersects"
        f"&outFields=FLD_ZONE,DFIRM_ID"
        f"&returnGeometry=false&f=json"
    )
    resp = requests.get(url, timeout=timeout)
    resp.raise_for_status()
    data = resp.json()
    return data.get("features", [])


def get_flood_zone(lat: float, lng: float) -> dict:
    """
    Returns flood zone info for a lat/lng coordinate.
    Tries multiple FEMA endpoints — falls back gracefully with manual lookup link.
    """
    endpoints = [
        (FEMA_PRIMARY,   15),
        (FEMA_MIRROR_1,  20),
        (FEMA_MIRROR_2,  20),
    ]

    last_err = None
    for url, timeout in endpoints:
        try:
            features = _query_fema(url, lat, lng, timeout)

            if not features:
                return {
                    "zone": "X",
                    "description": "No flood hazard data — likely Zone X (minimal risk)",
                    "firm_panel": "N/A",
                    "flood_insurance_required": False,
                    "source": "FEMA NFHL REST API",
                    "source_url": f"https://msc.fema.gov/portal/home",
                    "note": "No features returned — defaulting to X. Verify at MSC.",
                }

            attrs = features[0].get("attributes", {})
            zone  = attrs.get("FLD_ZONE", "X")
            firm  = attrs.get("DFIRM_ID", "N/A")

            return {
                "zone": zone,
                "description": ZONE_DESCRIPTIONS.get(zone, f"Flood zone {zone}"),
                "firm_panel": firm,
                "flood_insurance_required": zone not in ("X", "D"),
                "source": "FEMA National Flood Hazard Layer (NFHL) REST API",
                "source_url": "https://msc.fema.gov/portal/home",
            }

        except Exception as e:
            last_err = str(e)
            continue  # try next endpoint

    # All endpoints failed — return graceful fallback with manual lookup link
    verify_url = f"https://msc.fema.gov/portal/home"
    return {
        "zone": "X",
        "description": "Could not retrieve — defaulting to Zone X. Verify before closing.",
        "firm_panel": "N/A",
        "flood_insurance_required": False,
        "flood_lookup_url": verify_url,
        "source": "FEMA NFHL (lookup failed — manual verification required)",
        "source_url": verify_url,
        "error": last_err,
        "note": "FEMA API unavailable from server. Verify at msc.fema.gov before closing.",
    }
