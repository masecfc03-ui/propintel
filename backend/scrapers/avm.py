"""
PropIntel Internal AVM (Automated Valuation Model)
Built from comparable sales data already pulled from Regrid/Realie.
No external paid APIs required.
"""

import logging
import math
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

log = logging.getLogger(__name__)


def calculate_avm(subject_property: dict, comps: list) -> dict:
    """
    Build AVM from comparable sales already pulled from Regrid/Realie.
    
    Logic:
    1. Filter comps: same property type, within 0.5mi, sold within 12 months, ±20% sqft
    2. Calculate price/sqft for each comp
    3. Median price/sqft × subject sqft = base value
    4. Adjustments: +/- 2% per bed diff, +/- 1.5% per bath diff, +/- 0.5% per year age diff
    5. Range: base ±10%
    6. Confidence: "high" if 5+ comps, "medium" if 3-4, "low" if 1-2, "insufficient" if 0
    
    Returns:
    {
        "value": 425000,
        "value_low": 382500,
        "value_high": 467500,
        "price_per_sqft": 212,
        "confidence": "high",
        "comp_count": 7,
        "method": "internal_avm_v1"
    }
    """
    if not subject_property or not comps:
        return {
            "available": False,
            "error": "Subject property or comps data missing",
            "method": "internal_avm_v1"
        }
    
    # Extract subject property characteristics
    subject_sf = _get_building_sf(subject_property)
    subject_beds = _get_beds(subject_property)
    subject_baths = _get_baths(subject_property)
    subject_year = _get_year_built(subject_property)
    subject_prop_type = _classify_property_type(subject_property)
    subject_lat = subject_property.get("lat") or subject_property.get("latitude")
    subject_lng = subject_property.get("lng") or subject_property.get("longitude")
    
    if not subject_sf or subject_sf <= 0:
        return {
            "available": False,
            "error": "Subject property building square footage required",
            "method": "internal_avm_v1"
        }
    
    # Filter comparable sales
    filtered_comps = _filter_comps(
        comps, 
        subject_lat, 
        subject_lng,
        subject_sf, 
        subject_prop_type
    )
    
    if not filtered_comps:
        return {
            "available": False,
            "error": "No suitable comparable sales found after filtering",
            "method": "internal_avm_v1",
            "comp_count": 0,
            "confidence": "insufficient"
        }
    
    # Calculate price per square foot for each comp
    comp_psf_values = []
    valid_comps = []
    
    for comp in filtered_comps:
        comp_sf = _get_building_sf(comp)
        comp_price = _get_sale_price(comp)
        
        if comp_sf and comp_sf > 0 and comp_price and comp_price > 0:
            psf = comp_price / comp_sf
            comp_psf_values.append(psf)
            valid_comps.append({
                **comp,
                "price_per_sf": psf,
                "building_sf": comp_sf,
                "sale_price": comp_price
            })
    
    if not comp_psf_values:
        return {
            "available": False,
            "error": "No comps with valid price and square footage data",
            "method": "internal_avm_v1",
            "comp_count": 0,
            "confidence": "insufficient"
        }
    
    # Calculate median price per square foot
    median_psf = _calculate_median(comp_psf_values)
    
    # Calculate base value (median psf × subject sqft)
    base_value = median_psf * subject_sf
    
    # Apply adjustments for property characteristics
    adjusted_value = _apply_adjustments(
        base_value, 
        subject_beds, 
        subject_baths, 
        subject_year,
        valid_comps
    )
    
    # Calculate value range (±10%)
    value_low = round(adjusted_value * 0.9, 0)
    value_high = round(adjusted_value * 1.1, 0)
    
    # Determine confidence level
    comp_count = len(valid_comps)
    if comp_count >= 5:
        confidence = "high"
    elif comp_count >= 3:
        confidence = "medium"
    elif comp_count >= 1:
        confidence = "low"
    else:
        confidence = "insufficient"
    
    return {
        "available": True,
        "value": round(adjusted_value, 0),
        "value_low": value_low,
        "value_high": value_high,
        "value_fmt": f"${adjusted_value:,.0f}",
        "range_fmt": f"${value_low:,.0f} – ${value_high:,.0f}",
        "price_per_sqft": round(median_psf, 0),
        "confidence": confidence,
        "comp_count": comp_count,
        "method": "internal_avm_v1",
        "source": "PropIntel Internal AVM",
        "basis": f"Based on {comp_count} comparable sales",
        "comps_used": valid_comps[:5]  # Return top 5 comps for reference
    }


def _get_building_sf(prop: dict) -> Optional[float]:
    """Extract building square footage from property dict."""
    sf_fields = [
        'building_sf', 'buildingArea', 'sqft', 'building_area',
        'll_bldg_footprint_sqft', 'universalSize', 'livingSize'
    ]
    
    for field in sf_fields:
        value = prop.get(field)
        if value:
            try:
                # Handle comma-separated strings
                if isinstance(value, str):
                    value = value.replace(',', '')
                return float(value)
            except (ValueError, TypeError):
                continue
    
    return None


def _get_beds(prop: dict) -> Optional[int]:
    """Extract bedroom count from property dict."""
    bed_fields = [
        'beds', 'bedrooms', 'bedsCount', 'total_bedrooms', 
        'totalBedrooms'
    ]
    
    for field in bed_fields:
        value = prop.get(field)
        if value is not None:
            try:
                return int(float(value))
            except (ValueError, TypeError):
                continue
    
    return None


def _get_baths(prop: dict) -> Optional[float]:
    """Extract bathroom count from property dict."""
    bath_fields = [
        'baths', 'bathrooms', 'bathsTotal', 'bathsFullCalc',
        'total_bathrooms', 'totalBathrooms'
    ]
    
    for field in bath_fields:
        value = prop.get(field)
        if value is not None:
            try:
                return float(value)
            except (ValueError, TypeError):
                continue
    
    return None


def _get_year_built(prop: dict) -> Optional[int]:
    """Extract year built from property dict."""
    year_fields = ['year_built', 'yearBuilt', 'yearbuilt']
    
    for field in year_fields:
        value = prop.get(field)
        if value:
            try:
                year = int(value)
                # Validate reasonable year range
                if 1800 <= year <= datetime.now().year:
                    return year
            except (ValueError, TypeError):
                continue
    
    return None


def _get_sale_price(comp: dict) -> Optional[float]:
    """Extract sale price from comparable sale."""
    price_fields = [
        'sale_amount', 'saleAmount', 'sale_price', 'price', 
        'saleAmt', 'sale_value'
    ]
    
    for field in price_fields:
        value = comp.get(field)
        if value:
            try:
                if isinstance(value, str):
                    value = value.replace(',', '').replace('$', '')
                return float(value)
            except (ValueError, TypeError):
                continue
    
    return None


def _classify_property_type(prop: dict) -> str:
    """Classify property type for comparison filtering."""
    use_desc = (prop.get('use_description') or '').upper()
    prop_type = (prop.get('property_type') or '').upper()
    use_type = (prop.get('use_type') or '').upper()
    
    # Combine all type fields
    type_text = f"{use_desc} {prop_type} {use_type}".upper()
    
    if any(keyword in type_text for keyword in [
        'SINGLE FAMILY', 'RESIDENTIAL', 'SFR', 'RESIDENCE',
        'CONDO', 'TOWNHOME', 'DUPLEX', 'TRIPLEX', 'FOURPLEX'
    ]):
        return 'RESIDENTIAL'
    elif any(keyword in type_text for keyword in [
        'MULTI', 'APARTMENT', 'MULTIFAMILY'
    ]):
        return 'MULTIFAMILY'
    elif any(keyword in type_text for keyword in [
        'COMMERCIAL', 'OFFICE', 'RETAIL', 'SHOPPING'
    ]):
        return 'COMMERCIAL'
    elif any(keyword in type_text for keyword in [
        'INDUSTRIAL', 'WAREHOUSE', 'MANUFACTURING'
    ]):
        return 'INDUSTRIAL'
    else:
        return 'RESIDENTIAL'  # Default assumption


def _filter_comps(comps: List[dict], 
                  subject_lat: Optional[float], 
                  subject_lng: Optional[float],
                  subject_sf: float, 
                  subject_type: str) -> List[dict]:
    """Filter comps based on distance, time, size, and property type."""
    filtered = []
    cutoff_date = datetime.now() - timedelta(days=365)  # 12 months
    min_sf = subject_sf * 0.8  # -20%
    max_sf = subject_sf * 1.2  # +20%
    
    for comp in comps:
        # Check property type match
        comp_type = _classify_property_type(comp)
        if comp_type != subject_type:
            continue
        
        # Check sale date (within 12 months)
        sale_date = _parse_sale_date(comp)
        if sale_date and sale_date < cutoff_date:
            continue
        
        # Check building size (±20%)
        comp_sf = _get_building_sf(comp)
        if not comp_sf or comp_sf < min_sf or comp_sf > max_sf:
            continue
        
        # Check distance (within 0.5 miles if coordinates available)
        if subject_lat and subject_lng:
            comp_lat = comp.get('lat') or comp.get('latitude')
            comp_lng = comp.get('lng') or comp.get('longitude')
            
            if comp_lat and comp_lng:
                distance = _calculate_distance(
                    subject_lat, subject_lng, 
                    comp_lat, comp_lng
                )
                if distance > 0.5:  # 0.5 miles
                    continue
        
        # Comp passed all filters
        filtered.append(comp)
    
    return filtered


def _parse_sale_date(comp: dict) -> Optional[datetime]:
    """Parse sale date from various formats."""
    date_fields = ['sale_date', 'saleDate', 'sold_date', 'saleTransDate']
    
    for field in date_fields:
        date_str = comp.get(field)
        if not date_str:
            continue
        
        try:
            # Handle various date formats
            if isinstance(date_str, str):
                # ISO format: 2024-01-15
                if len(date_str) >= 10:
                    return datetime.strptime(date_str[:10], '%Y-%m-%d')
            elif hasattr(date_str, 'year'):
                # Already a datetime object
                return date_str
        except (ValueError, TypeError):
            continue
    
    return None


def _calculate_distance(lat1: float, lng1: float, 
                       lat2: float, lng2: float) -> float:
    """Calculate distance between two points in miles using Haversine formula."""
    # Convert latitude and longitude from degrees to radians
    lat1, lng1, lat2, lng2 = map(math.radians, [lat1, lng1, lat2, lng2])
    
    # Haversine formula
    dlat = lat2 - lat1
    dlng = lng2 - lng1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlng/2)**2
    c = 2 * math.asin(math.sqrt(a))
    
    # Radius of earth in miles
    r = 3956
    
    return c * r


def _calculate_median(values: List[float]) -> float:
    """Calculate median of a list of values."""
    sorted_values = sorted(values)
    n = len(sorted_values)
    
    if n % 2 == 0:
        # Even number of values
        return (sorted_values[n//2 - 1] + sorted_values[n//2]) / 2
    else:
        # Odd number of values
        return sorted_values[n//2]


def _apply_adjustments(base_value: float,
                      subject_beds: Optional[int],
                      subject_baths: Optional[float], 
                      subject_year: Optional[int],
                      comps: List[dict]) -> float:
    """Apply adjustments for bed/bath/age differences vs comparable average."""
    if not comps:
        return base_value
    
    # Calculate average characteristics of comps
    comp_beds = [_get_beds(c) for c in comps if _get_beds(c) is not None]
    comp_baths = [_get_baths(c) for c in comps if _get_baths(c) is not None]
    comp_years = [_get_year_built(c) for c in comps if _get_year_built(c) is not None]
    
    adjusted_value = base_value
    
    # Bedroom adjustment: +/- 2% per bedroom difference
    if subject_beds is not None and comp_beds:
        avg_comp_beds = sum(comp_beds) / len(comp_beds)
        bed_diff = subject_beds - avg_comp_beds
        bed_adjustment = bed_diff * 0.02  # 2% per bedroom
        adjusted_value *= (1 + bed_adjustment)
    
    # Bathroom adjustment: +/- 1.5% per bathroom difference
    if subject_baths is not None and comp_baths:
        avg_comp_baths = sum(comp_baths) / len(comp_baths)
        bath_diff = subject_baths - avg_comp_baths
        bath_adjustment = bath_diff * 0.015  # 1.5% per bathroom
        adjusted_value *= (1 + bath_adjustment)
    
    # Age adjustment: +/- 0.5% per year difference
    if subject_year is not None and comp_years:
        current_year = datetime.now().year
        subject_age = current_year - subject_year
        avg_comp_year = sum(comp_years) / len(comp_years)
        avg_comp_age = current_year - avg_comp_year
        
        age_diff = avg_comp_age - subject_age  # Newer is better
        age_adjustment = age_diff * 0.005  # 0.5% per year
        adjusted_value *= (1 + age_adjustment)
    
    return adjusted_value