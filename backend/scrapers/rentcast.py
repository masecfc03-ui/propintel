#!/usr/bin/env python3
"""
RentCast API Integration for PropIntel

This script integrates with RentCast API to fetch property details and AVM estimates.
Requires a free API key from https://app.rentcast.io/app/api

API Documentation: https://developers.rentcast.io
"""

import requests
import json
import os
from typing import Dict, Optional


class RentCastAPI:
    """RentCast API client for property data and valuations."""
    
    def __init__(self, api_key: str):
        """Initialize RentCast API client.
        
        Args:
            api_key: RentCast API key from your account dashboard
        """
        self.api_key = api_key
        self.base_url = "https://api.rentcast.io/v1"
        self.headers = {
            "Accept": "application/json",
            "X-Api-Key": api_key
        }
    
    def get_property_details(self, address: str) -> Optional[Dict]:
        """Fetch property details by address.
        
        Args:
            address: Full property address (e.g., "1234 Main St, Dallas TX 75201")
            
        Returns:
            Property details dictionary or None if not found
        """
        try:
            # Parse address components (basic implementation)
            parts = address.split(',')
            street = parts[0].strip()
            
            if len(parts) >= 2:
                city_state = parts[1].strip()
                city_parts = city_state.split()
                if len(city_parts) >= 2:
                    state = city_parts[-1]
                    city = ' '.join(city_parts[:-1])
                else:
                    city = city_state
                    state = None
            else:
                city = None
                state = None
            
            # Build query parameters
            params = {}
            if street:
                params['address'] = street
            if city:
                params['city'] = city
            if state:
                params['state'] = state
                
            response = requests.get(
                f"{self.base_url}/properties",
                headers=self.headers,
                params=params,
                timeout=30
            )
            
            response.raise_for_status()
            data = response.json()
            
            # Return first property if found
            if isinstance(data, list) and len(data) > 0:
                return data[0]
            elif isinstance(data, dict) and 'properties' in data:
                properties = data['properties']
                if len(properties) > 0:
                    return properties[0]
            
            return None
            
        except requests.RequestException as e:
            print(f"Error fetching property details: {e}")
            return None
    
    def get_avm_estimate(self, address: str) -> Optional[Dict]:
        """Fetch AVM (Automated Valuation Model) estimate for a property.
        
        Args:
            address: Full property address
            
        Returns:
            AVM estimate dictionary or None if not available
        """
        try:
            params = {'address': address}
            
            response = requests.get(
                f"{self.base_url}/avm/value",
                headers=self.headers,
                params=params,
                timeout=30
            )
            
            response.raise_for_status()
            return response.json()
            
        except requests.RequestException as e:
            print(f"Error fetching AVM estimate: {e}")
            return None
    
    def get_property_intelligence(self, address: str) -> Dict:
        """Get comprehensive property intelligence combining property details and AVM.
        
        Args:
            address: Full property address
            
        Returns:
            Structured dictionary with all available property data
        """
        print(f"Fetching property intelligence for: {address}")
        
        # Fetch property details
        property_details = self.get_property_details(address)
        avm_estimate = self.get_avm_estimate(address)
        
        # Build structured response
        result = {
            "address": address,
            "property_found": property_details is not None,
            "avm_found": avm_estimate is not None,
            "value": None,
            "value_range_low": None,
            "value_range_high": None,
            "rent_estimate": None,
            "bedrooms": None,
            "bathrooms": None,
            "sqft": None,
            "year_built": None,
            "raw_property_data": property_details,
            "raw_avm_data": avm_estimate
        }
        
        # Extract property details
        if property_details:
            result["bedrooms"] = property_details.get("bedrooms")
            result["bathrooms"] = property_details.get("bathrooms") 
            result["sqft"] = property_details.get("squareFootage") or property_details.get("sqft")
            result["year_built"] = property_details.get("yearBuilt") or property_details.get("year_built")
            
        # Extract AVM data
        if avm_estimate:
            result["value"] = avm_estimate.get("value") or avm_estimate.get("price")
            result["value_range_low"] = avm_estimate.get("valueLow") or avm_estimate.get("value_low")
            result["value_range_high"] = avm_estimate.get("valueHigh") or avm_estimate.get("value_high")
            result["rent_estimate"] = avm_estimate.get("rentEstimate") or avm_estimate.get("rent")
        
        return result


def test_rentcast_api():
    """Test the RentCast API with a sample address."""
    # Try to get API key from environment variable
    api_key = os.getenv("RENTCAST_API_KEY")
    
    if not api_key:
        print("ERROR: RENTCAST_API_KEY environment variable not set")
        print("Please sign up at https://app.rentcast.io/app/api and set your API key")
        return None
    
    client = RentCastAPI(api_key)
    test_address = "1234 Main St, Dallas TX 75201"
    
    result = client.get_property_intelligence(test_address)
    
    return result


if __name__ == "__main__":
    # Run test if executed directly
    result = test_rentcast_api()
    
    if result:
        print("\n" + "="*50)
        print("RENTCAST TEST RESULTS")
        print("="*50)
        print(json.dumps(result, indent=2))
        
        # Save results to file
        output_path = "/Users/masonmathis/.openclaw/workspace/deallens/backend/scrapers/rentcast_test_results.json"
        with open(output_path, 'w') as f:
            json.dump(result, f, indent=2)
        print(f"\nResults saved to: {output_path}")
    else:
        print("Test failed - check API key and connection")