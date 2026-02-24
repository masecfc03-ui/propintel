"""
Tracerfy Skip Trace Integration
Submits single-record CSV to Tracerfy API, polls for completion, returns contact data.

API docs: https://tracerfy.com/api
Usage: set TRACERFY_API_KEY in .env or ~/.openclaw/.secure/tracerfy-api-key.txt

Cost: ~15 credits per enhanced trace (~$0.135), 1 credit per standard trace (~$0.009)
"""
import os
import csv
import io
import time
import requests
from pathlib import Path
from typing import Optional

def _load_api_key() -> str:
    """Load Tracerfy API key from env var or secure file."""
    # First try environment variable
    api_key = os.environ.get("TRACERFY_API_KEY", "")
    if api_key:
        return api_key
    
    # Fallback to secure file
    try:
        secure_file = Path.home() / ".openclaw" / ".secure" / "tracerfy-api-key.txt"
        if secure_file.exists():
            return secure_file.read_text().strip()
    except Exception:
        pass
    
    return ""

TRACERFY_API_KEY = _load_api_key()
TRACERFY_API_BASE = "https://app.fastappend.com/v1/api"

def skip_trace(owner_name: str, mailing_address: str, city: str, 
               state: str, zip_code: str) -> dict:
    """
    Run skip trace on a property owner via Tracerfy API.
    
    Args:
        owner_name: Full owner name (e.g., "MASON MATHIS" or "SMITH FAMILY TRUST")
        mailing_address: Owner's mailing address
        city: Mailing city
        state: Mailing state (default TX)
        zip_code: Mailing ZIP code
        
    Returns:
        {
          "status": "hit" | "no_hit" | "error",
          "phones": ["9728347204", ...],
          "emails": ["owner@example.com", ...],
          "source": "Tracerfy"
        }
    """
    if not TRACERFY_API_KEY:
        return {
            "status": "error",
            "phones": [],
            "emails": [],
            "source": "Tracerfy", 
            "error": "Tracerfy API key not configured"
        }
    
    if not owner_name:
        return {
            "status": "error",
            "phones": [],
            "emails": [],
            "source": "Tracerfy",
            "error": "Owner name required"
        }
    
    # Parse owner name into first/last
    first_name, last_name = _parse_owner_name(owner_name)
    
    if not first_name and not last_name:
        return {
            "status": "error", 
            "phones": [],
            "emails": [],
            "source": "Tracerfy",
            "error": "Could not parse owner name"
        }
    
    try:
        # Create single-record CSV
        csv_data = _create_csv(first_name, last_name, mailing_address, city, state, zip_code)
        
        # Submit job
        job_id = _submit_job(csv_data)
        if not job_id:
            return {
                "status": "error",
                "phones": [],
                "emails": [],
                "source": "Tracerfy",
                "error": "Failed to submit job"
            }
        
        # Wait for completion (max 30 seconds)
        result = _wait_for_completion(job_id, max_wait=30)
        if not result:
            return {
                "status": "error",
                "phones": [],
                "emails": [],
                "source": "Tracerfy",
                "error": "Job timeout or failed"
            }
        
        # Parse results
        phones, emails = _parse_results(result)
        
        if phones or emails:
            return {
                "status": "hit",
                "phones": phones,
                "emails": emails,
                "source": "Tracerfy"
            }
        else:
            return {
                "status": "no_hit", 
                "phones": [],
                "emails": [],
                "source": "Tracerfy"
            }
            
    except Exception as e:
        return {
            "status": "error",
            "phones": [],
            "emails": [],
            "source": "Tracerfy",
            "error": str(e)
        }

def _parse_owner_name(owner_name: str) -> tuple:
    """
    Parse owner name into first and last name.
    Handles trust names, multiple names, etc.
    """
    if not owner_name:
        return "", ""
    
    # Convert to string and handle None values
    owner_name = str(owner_name or '').strip()
    if not owner_name:
        return "", ""
    
    # Remove common trust suffixes for name parsing
    name = owner_name.replace(' TRUSTEE', '').replace(' TRUSTEES', '').replace(' TRUST', '')
    name = name.replace(',', '').strip()
    
    parts = name.split()
    if len(parts) == 0:
        return "", ""
    elif len(parts) == 1:
        return parts[0], ""
    elif len(parts) == 2:
        return parts[0], parts[1]
    else:
        # Multiple names - use first word as first name, rest as last
        return parts[0], ' '.join(parts[1:])

def _create_csv(first_name: str, last_name: str, address: str, city: str, state: str, zip_code: str) -> str:
    """Create CSV string for Tracerfy API."""
    output = []
    header = ['address', 'city', 'state', 'owner_first_name', 'owner_last_name']
    output.append(','.join(header))
    
    row = [
        f'"{address}"',
        f'"{city}"', 
        f'"{state}"',
        f'"{first_name}"',
        f'"{last_name}"'
    ]
    output.append(','.join(row))
    
    return '\n'.join(output)

def _submit_job(csv_data: str) -> Optional[str]:
    """Submit skip tracing job to Tracerfy API."""
    try:
        headers = {
            "Authorization": f"Bearer {TRACERFY_API_KEY}",
        }
        
        files = {
            'file': ('leads.csv', csv_data, 'text/csv')
        }
        
        data = {
            'enhanced': 'false'  # Use standard tracing (1 credit vs 15 for enhanced)
        }
        
        response = requests.post(
            f"{TRACERFY_API_BASE}/trace/", 
            files=files, 
            data=data, 
            headers=headers,
            timeout=30
        )
        
        if response.status_code in [200, 201]:
            result = response.json()
            return result.get('job_id') or result.get('id')
        else:
            print(f"Tracerfy submit error: {response.status_code} - {response.text}")
            return None
            
    except Exception as e:
        print(f"Tracerfy submit exception: {e}")
        return None

def _wait_for_completion(job_id: str, max_wait: int = 30) -> Optional[dict]:
    """Wait for job completion, polling every 3 seconds."""
    headers = {
        "Authorization": f"Bearer {TRACERFY_API_KEY}",
    }
    
    start_time = time.time()
    
    while time.time() - start_time < max_wait:
        try:
            response = requests.get(
                f"{TRACERFY_API_BASE}/queue/{job_id}",
                headers=headers,
                timeout=10
            )
            
            if response.status_code == 200:
                status = response.json()
                
                if status.get('status') == 'completed':
                    return status
                elif status.get('status') == 'failed':
                    return None
                
                # Still processing, wait and retry
                time.sleep(3)
            else:
                return None
                
        except Exception:
            time.sleep(3)
    
    return None

def _parse_results(result_data: dict) -> tuple:
    """Parse phones and emails from Tracerfy results."""
    phones = []
    emails = []
    
    try:
        # Get CSV results data
        results_csv = result_data.get('results_csv') or result_data.get('data')
        if not results_csv:
            return phones, emails
        
        # Parse CSV
        lines = results_csv.strip().split('\n')
        if len(lines) > 1:  # Has header and data
            header = [h.strip().lower() for h in lines[0].split(',')]
            for line in lines[1:]:
                values = [v.strip().strip('"') for v in line.split(',')]
                if len(values) >= len(header):
                    result_dict = dict(zip(header, values))
                    
                    # Extract phone numbers
                    for phone_field in ['phone_1', 'phone_2', 'phone_3', 'phone', 'cell_phone']:
                        phone = result_dict.get(phone_field, '').strip()
                        if phone and len(phone) >= 10 and phone not in phones:
                            phones.append(phone)
                    
                    # Extract emails
                    for email_field in ['email_1', 'email_2', 'email', 'email_address']:
                        email = result_dict.get(email_field, '').strip().lower()
                        if email and '@' in email and email not in emails:
                            emails.append(email)
        
    except Exception as e:
        print(f"Error parsing Tracerfy results: {e}")
    
    return phones, emails