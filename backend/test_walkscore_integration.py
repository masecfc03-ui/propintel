#!/usr/bin/env python3
"""
Test script for Walk Score integration
Run with: python3 test_walkscore_integration.py
"""

import os
from scrapers.walkscore import get_scores

def test_walkscore_integration():
    """Test Walk Score API integration"""
    
    # Test with sample Dallas address
    address = "123 Main St, Dallas, TX"
    lat = 32.7767
    lng = -96.7970
    
    print(f"Testing Walk Score integration for: {address}")
    print(f"Coordinates: {lat}, {lng}")
    
    # Check if API key is configured
    api_key = os.getenv("WALKSCORE_API_KEY", "")
    if api_key and api_key != "YOUR_WALKSCORE_API_KEY_HERE":
        print("✅ WALKSCORE_API_KEY is configured")
    else:
        print("❌ WALKSCORE_API_KEY is not configured or is placeholder")
        print("   Register at: https://www.walkscore.com/professional/api-sign-up.php")
        print("   Use: masecfc03@gmail.com, propertyvalueintel.com")
        print("   Description: 'Property intelligence reports for real estate agents'")
    
    # Test the scraper
    result = get_scores(address, lat, lng)
    
    print("\nResult:")
    print(f"Available: {result.get('available', 'Unknown')}")
    
    if result.get("available"):
        print(f"Walk Score: {result.get('walk_score', 'N/A')}")
        print(f"Walk Description: {result.get('walk_description', 'N/A')}")
        print(f"Transit Score: {result.get('transit_score', 'N/A')}")
        print(f"Transit Description: {result.get('transit_description', 'N/A')}")
        print(f"Bike Score: {result.get('bike_score', 'N/A')}")
        print(f"Bike Description: {result.get('bike_description', 'N/A')}")
    else:
        print(f"Note: {result.get('note', 'Unknown error')}")
        if 'error' in result:
            print(f"Error: {result['error']}")

if __name__ == "__main__":
    test_walkscore_integration()