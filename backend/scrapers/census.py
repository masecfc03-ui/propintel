"""
Census ACS 5-Year Estimates — Demographics by ZIP code.
Free API. No key required for basic use.
Source: https://api.census.gov/data/2023/acs/acs5
Variables: https://api.census.gov/data/2023/acs/acs5/variables.html
"""
import requests

CENSUS_URL = "https://api.census.gov/data/2022/acs/acs5"

VARIABLES = {
    "B01003_001E": "population",
    "B19013_001E": "median_household_income",
    "B25003_002E": "owner_occupied_units",
    "B25003_001E": "total_occupied_units",
    "B01002_001E": "median_age",
    "B23025_002E": "labor_force",
    "B23025_004E": "employed",
}

def get_demographics(zip_code: str) -> dict:
    """
    Returns demographic data for a ZIP code.
    {
      "zip": "75042",
      "population": 39148,
      "median_household_income": 64266,
      "owner_occupied_pct": 51.3,
      "median_age": 32.4,
      "source": "U.S. Census Bureau, ACS 5-Year Estimates 2022"
    }
    """
    try:
        var_list = ",".join(VARIABLES.keys())
        params = {
            "get": var_list,
            "for": f"zip code tabulation area:{zip_code}",
            "key": ""  # optional — add key for production
        }
        resp = requests.get(CENSUS_URL, params=params, timeout=12)
        resp.raise_for_status()
        data = resp.json()

        if len(data) < 2:
            return {"error": "No census data for this ZIP", "zip": zip_code}

        headers = data[0]
        values = data[1]
        row = dict(zip(headers, values))

        pop = int(row.get("B01003_001E", 0) or 0)
        mhi = int(row.get("B19013_001E", 0) or 0)
        owner = int(row.get("B25003_002E", 0) or 0)
        total_occ = int(row.get("B25003_001E", 1) or 1)
        median_age = float(row.get("B01002_001E", 0) or 0)
        labor = int(row.get("B23025_002E", 0) or 0)
        employed = int(row.get("B23025_004E", 0) or 0)

        owner_pct = round((owner / total_occ * 100), 1) if total_occ > 0 else None
        unemployment_rate = round(((labor - employed) / labor * 100), 1) if labor > 0 else None

        return {
            "zip": zip_code,
            "population": pop,
            "median_household_income": mhi,
            "median_household_income_fmt": f"${mhi:,}",
            "owner_occupied_units": owner,
            "total_occupied_units": total_occ,
            "owner_occupied_pct": owner_pct,
            "median_age": median_age,
            "unemployment_rate": unemployment_rate,
            "source": "U.S. Census Bureau, ACS 5-Year Estimates 2022",
            "source_url": f"https://data.census.gov/table?q=DP03&g=860XX00US{zip_code}",
        }

    except Exception as e:
        return {"error": str(e), "zip": zip_code}
