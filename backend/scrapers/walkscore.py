"""
Walk Score API — Walkability, Transit, Bike scores for any US address.
Free tier: 5,000 calls/day

Register at: https://www.walkscore.com/professional/api.php
API docs:   https://www.walkscore.com/professional/api-sample-code.php

Fields returned:
  - walk_score (0-100) + walk_description
  - transit_score (0-100) + transit_description
  - bike_score (0-100) + bike_description
  - logo_url, more_info_link
"""
import os
import requests

WALKSCORE_KEY = os.getenv("WALKSCORE_API_KEY", "")
BASE_URL = "https://api.walkscore.com/score"


def get_scores(address: str, lat: float, lng: float) -> dict:
    """
    Fetch Walk Score, Transit Score, and Bike Score for a given address + coordinates.

    Returns:
        dict with walk_score, transit_score, bike_score, descriptions, and status.
        On error: dict with 'error' key.
    """
    if not WALKSCORE_KEY:
        return {
            "available": False,
            "note": "Walk Score API key not configured (WALKSCORE_API_KEY env var)",
        }

    try:
        resp = requests.get(
            BASE_URL,
            params={
                "format": "json",
                "address": address,
                "lat": lat,
                "lon": lng,
                "transit": 1,
                "bike": 1,
                "wsapikey": WALKSCORE_KEY,
            },
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
    except requests.exceptions.Timeout:
        return {"available": False, "error": "Walk Score API timeout"}
    except requests.exceptions.HTTPError as e:
        return {"available": False, "error": f"Walk Score HTTP {e.response.status_code if e.response else 0}"}
    except Exception as e:
        return {"available": False, "error": str(e)}

    status = data.get("status", 0)
    if status not in (1, 2):  # 1 = score, 2 = score from polygon
        return {"available": False, "note": "No Walk Score data for this address"}

    def score_label(score, thresholds):
        """Convert numeric score to descriptive label."""
        for threshold, label in thresholds:
            if score >= threshold:
                return label
        return "N/A"

    walk_thresholds = [(90,"Walker's Paradise"),(70,"Very Walkable"),(50,"Somewhat Walkable"),(25,"Car-Dependent"),(0,"Almost All Errands Require a Car")]
    transit_thresholds = [(90,"Rider's Paradise"),(70,"Excellent Transit"),(50,"Good Transit"),(25,"Some Transit"),(0,"Minimal Transit")]
    bike_thresholds = [(90,"Biker's Paradise"),(70,"Very Bikeable"),(50,"Bikeable"),(0,"Minimal Bike Infrastructure")]

    walk  = data.get("walkscore")
    trans = (data.get("transit") or {}).get("score")
    bike  = (data.get("bike") or {}).get("score")

    result = {
        "available": True,
        "status": status,
        "walk_score":  walk,
        "walk_description":  data.get("description") or (score_label(walk, walk_thresholds) if walk is not None else None),
        "transit_score": trans,
        "transit_description": ((data.get("transit") or {}).get("summary")) or (score_label(trans, transit_thresholds) if trans is not None else None),
        "bike_score": bike,
        "bike_description": ((data.get("bike") or {}).get("summary")) or (score_label(bike, bike_thresholds) if bike is not None else None),
        "more_info": data.get("more_info_link"),
        "logo_url": data.get("logo_url"),
    }

    return result
