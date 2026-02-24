#!/usr/bin/env python3
"""
Buffer Post Tool - Queue social media posts via Buffer API

Post media URLs + captions to Buffer for scheduling across multiple platforms.
Supports Instagram, Twitter, TikTok and other Buffer-supported platforms.

Usage:
    python3 buffer_post.py "https://example.com/image.jpg" "Check out this property!" --platforms instagram twitter
    python3 buffer_post.py --url "https://example.com/video.mp4" --caption "New listing alert! 🏠" --platforms tiktok
    
Environment Variables Required:
    BUFFER_ACCESS_TOKEN - Buffer API access token
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import List, Optional

import requests

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("buffer_post")

# Buffer API configuration
BUFFER_API_BASE = "https://api.bufferapp.com/1"
SUPPORTED_PLATFORMS = {
    'instagram', 'twitter', 'tiktok', 'facebook', 'linkedin', 
    'pinterest', 'shopify', 'googlebusiness'
}


class BufferClient:
    """Buffer API client for posting content"""
    
    def __init__(self):
        self.access_token = os.getenv('BUFFER_ACCESS_TOKEN')
        
        if not self.access_token:
            raise ValueError("Missing required environment variable: BUFFER_ACCESS_TOKEN")
        
        self.headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json'
        }
        
        # Cache for profile data
        self._profiles = None
    
    def get_profiles(self) -> List[dict]:
        """Fetch all connected Buffer profiles"""
        if self._profiles is not None:
            return self._profiles
            
        try:
            log.info("Fetching Buffer profiles...")
            response = requests.get(
                f"{BUFFER_API_BASE}/profiles.json",
                headers=self.headers,
                timeout=30
            )
            response.raise_for_status()
            
            self._profiles = response.json()
            log.info(f"Found {len(self._profiles)} connected profiles")
            return self._profiles
            
        except requests.exceptions.RequestException as e:
            raise Exception(f"Failed to fetch Buffer profiles: {str(e)}")
    
    def get_profiles_by_service(self, service_names: List[str]) -> List[dict]:
        """Get profiles filtered by service names (e.g., 'instagram', 'twitter')"""
        all_profiles = self.get_profiles()
        matching_profiles = []
        
        for profile in all_profiles:
            service = profile.get('service', '').lower()
            if service in [s.lower() for s in service_names]:
                matching_profiles.append(profile)
        
        return matching_profiles
    
    def create_update(self, text: str, media_url: Optional[str] = None, 
                     profile_ids: List[str] = None) -> dict:
        """
        Create a Buffer update (post)
        
        Args:
            text: Post caption/text
            media_url: Optional media URL to attach
            profile_ids: List of profile IDs to post to
            
        Returns:
            dict: Buffer API response with post details
        """
        if not profile_ids:
            raise ValueError("No profile IDs provided")
        
        # Prepare update data
        update_data = {
            'text': text,
            'profile_ids[]': profile_ids  # Buffer expects this format for multiple profiles
        }
        
        # Add media if provided
        if media_url:
            update_data['media'] = {
                'link': media_url,
                'title': 'Property Report Media'
            }
        
        try:
            log.info(f"Creating Buffer update for {len(profile_ids)} profiles...")
            log.debug(f"Update data: {update_data}")
            
            response = requests.post(
                f"{BUFFER_API_BASE}/updates/create.json",
                headers=self.headers,
                json=update_data,
                timeout=30
            )
            response.raise_for_status()
            
            result = response.json()
            log.info(f"✅ Buffer update created successfully")
            
            return result
            
        except requests.exceptions.RequestException as e:
            error_msg = f"Failed to create Buffer update: {str(e)}"
            
            # Try to extract more details from error response
            try:
                if hasattr(e.response, 'json'):
                    error_detail = e.response.json()
                    if 'error' in error_detail:
                        error_msg += f" - {error_detail['error']}"
            except:
                pass
                
            raise Exception(error_msg)
    
    def list_services(self) -> dict:
        """List available services and their details for debugging"""
        profiles = self.get_profiles()
        services = {}
        
        for profile in profiles:
            service = profile.get('service', 'unknown').lower()
            if service not in services:
                services[service] = {
                    'count': 0,
                    'profiles': []
                }
            
            services[service]['count'] += 1
            services[service]['profiles'].append({
                'id': profile.get('id'),
                'name': profile.get('formatted_username', profile.get('service_username', 'Unknown')),
                'active': not profile.get('disabled', False)
            })
        
        return services


def post_to_buffer(media_url: str, caption: str, platforms: List[str]) -> dict:
    """
    Post content to Buffer for the specified platforms
    
    Returns:
        dict: Result with keys: success, post_id, scheduled_time, platforms, error
    """
    result = {
        'success': False,
        'post_id': None,
        'scheduled_time': None,
        'platforms': platforms,
        'error': None,
        'created_at': datetime.now(timezone.utc).isoformat()
    }
    
    try:
        # Validate platforms
        invalid_platforms = [p for p in platforms if p.lower() not in SUPPORTED_PLATFORMS]
        if invalid_platforms:
            supported_list = ', '.join(sorted(SUPPORTED_PLATFORMS))
            raise ValueError(f"Unsupported platforms: {', '.join(invalid_platforms)}. "
                           f"Supported: {supported_list}")
        
        # Initialize Buffer client
        client = BufferClient()
        
        # Get matching profiles
        profiles = client.get_profiles_by_service(platforms)
        
        if not profiles:
            available_services = client.list_services()
            available_names = ', '.join(available_services.keys()) if available_services else 'none'
            raise ValueError(f"No Buffer profiles found for platforms: {', '.join(platforms)}. "
                           f"Available services: {available_names}")
        
        # Extract profile IDs
        profile_ids = [p['id'] for p in profiles if not p.get('disabled', False)]
        
        if not profile_ids:
            raise ValueError(f"All matching profiles are disabled for platforms: {', '.join(platforms)}")
        
        log.info(f"Posting to {len(profile_ids)} profiles across platforms: {', '.join(platforms)}")
        
        # Create the update
        buffer_response = client.create_update(
            text=caption,
            media_url=media_url,
            profile_ids=profile_ids
        )
        
        # Extract response data
        if 'updates' in buffer_response and buffer_response['updates']:
            # Multiple updates created
            first_update = buffer_response['updates'][0]
            result['post_id'] = first_update.get('id')
            result['scheduled_time'] = first_update.get('scheduled_at')
        else:
            # Single update response
            result['post_id'] = buffer_response.get('id')
            result['scheduled_time'] = buffer_response.get('scheduled_at')
        
        result['success'] = True
        
    except Exception as e:
        result['error'] = str(e)
        log.error(f"Buffer post failed: {e}")
    
    return result


def main():
    parser = argparse.ArgumentParser(description='Post content to Buffer for social media')
    
    # Media URL (positional or --url)
    parser.add_argument('media_url', nargs='?', help='Media URL to post (image/video)')
    parser.add_argument('--url', dest='media_url_alt', help='Alternative way to specify media URL')
    
    # Caption (positional or --caption)
    parser.add_argument('caption', nargs='?', help='Post caption/text')
    parser.add_argument('--caption', dest='caption_alt', help='Alternative way to specify caption')
    
    # Platforms
    parser.add_argument('--platforms', nargs='+', required=True, 
                       help='Platforms to post to (instagram twitter tiktok facebook linkedin)')
    
    # Output format
    parser.add_argument('--json', action='store_true',
                       help='Output result as JSON instead of human-readable format')
    
    # Debug/info commands
    parser.add_argument('--list-services', action='store_true',
                       help='List available Buffer services and profiles')
    
    args = parser.parse_args()
    
    try:
        # Handle list services command
        if args.list_services:
            client = BufferClient()
            services = client.list_services()
            
            if args.json:
                print(json.dumps(services, indent=2))
            else:
                print("\n📱 Available Buffer Services:")
                if not services:
                    print("   No connected profiles found")
                else:
                    for service, data in services.items():
                        print(f"\n   {service.upper()}: {data['count']} profile(s)")
                        for profile in data['profiles']:
                            status = "✅ active" if profile['active'] else "❌ disabled"
                            print(f"     • {profile['name']} ({profile['id']}) - {status}")
            return
        
        # Get media URL and caption
        media_url = args.media_url or args.media_url_alt
        caption = args.caption or args.caption_alt
        
        if not media_url:
            parser.error("Media URL is required (provide as argument or --url)")
        if not caption:
            parser.error("Caption is required (provide as argument or --caption)")
        
        # Post to Buffer
        result = post_to_buffer(media_url, caption, args.platforms)
        
        if args.json:
            print(json.dumps(result, indent=2))
        else:
            if result['success']:
                print(f"✅ Post queued successfully!")
                print(f"   Post ID: {result['post_id']}")
                if result['scheduled_time']:
                    print(f"   Scheduled: {result['scheduled_time']}")
                print(f"   Platforms: {', '.join(result['platforms'])}")
                print(f"   Media: {media_url}")
                print(f"   Caption: {caption[:100]}{'...' if len(caption) > 100 else ''}")
            else:
                print(f"❌ Post failed: {result['error']}")
                sys.exit(1)
                
    except KeyboardInterrupt:
        print("\n⚠️ Post interrupted by user")
        sys.exit(1)
    except Exception as e:
        log.error(f"Fatal error: {e}")
        if args.json:
            print(json.dumps({"error": str(e)}, indent=2))
        else:
            print(f"❌ Error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()