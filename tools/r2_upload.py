#!/usr/bin/env python3
"""
R2 Upload Tool - Upload screenshots/videos to Cloudflare R2 for social media posting

Upload individual files or batch upload from a directory.
Generates public URLs and logs all uploads to upload_log.jsonl

Usage:
    python3 r2_upload.py path/to/file.jpg
    python3 r2_upload.py --dir /path/to/screenshots/
    
Environment Variables Required:
    CF_R2_ACCOUNT_ID - Cloudflare R2 account ID
    CF_R2_ACCESS_KEY - R2 access key ID  
    CF_R2_SECRET_KEY - R2 secret access key
    CF_R2_BUCKET - R2 bucket name
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin

import boto3
from botocore.exceptions import BotoCoreError, ClientError

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("r2_upload")

# Supported file extensions
SUPPORTED_EXTENSIONS = {
    '.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp', '.svg',  # Images
    '.mp4', '.mov', '.avi', '.webm', '.mkv', '.m4v'            # Videos
}

# Upload log path
UPLOAD_LOG_PATH = Path(__file__).parent / "upload_log.jsonl"


class R2Uploader:
    """Handles Cloudflare R2 uploads using S3-compatible API"""
    
    def __init__(self):
        self.account_id = os.getenv('CF_R2_ACCOUNT_ID')
        self.access_key = os.getenv('CF_R2_ACCESS_KEY') 
        self.secret_key = os.getenv('CF_R2_SECRET_KEY')
        self.bucket = os.getenv('CF_R2_BUCKET')
        
        # Validate required environment variables
        missing_vars = []
        if not self.account_id:
            missing_vars.append('CF_R2_ACCOUNT_ID')
        if not self.access_key:
            missing_vars.append('CF_R2_ACCESS_KEY')
        if not self.secret_key:
            missing_vars.append('CF_R2_SECRET_KEY')
        if not self.bucket:
            missing_vars.append('CF_R2_BUCKET')
            
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
        
        # Initialize S3 client for R2
        self.s3_client = boto3.client(
            's3',
            endpoint_url=f'https://{self.account_id}.r2.cloudflarestorage.com',
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            region_name='auto'
        )
        
        self.public_url_base = f'https://pub-{self.bucket}.r2.dev'
        
    def upload_file(self, file_path: Path) -> dict:
        """
        Upload a single file to R2
        
        Returns:
            dict: Upload result with keys: filename, url, size, uploaded_at, success, error
        """
        result = {
            'filename': file_path.name,
            'size': 0,
            'uploaded_at': datetime.now(timezone.utc).isoformat(),
            'success': False,
            'error': None
        }
        
        try:
            # Check if file exists and get size
            if not file_path.exists():
                raise FileNotFoundError(f"File not found: {file_path}")
                
            file_size = file_path.stat().st_size
            result['size'] = file_size
            
            # Check file extension
            if file_path.suffix.lower() not in SUPPORTED_EXTENSIONS:
                raise ValueError(f"Unsupported file extension: {file_path.suffix}")
            
            # Generate unique object key: media/{timestamp}_{filename}
            timestamp = int(time.time() * 1000)  # milliseconds for uniqueness
            object_key = f"media/{timestamp}_{file_path.name}"
            
            # Determine content type
            content_type = self._get_content_type(file_path.suffix.lower())
            
            # Upload to R2
            log.info(f"Uploading {file_path.name} ({file_size:,} bytes) to R2...")
            
            with open(file_path, 'rb') as f:
                self.s3_client.upload_fileobj(
                    f,
                    self.bucket,
                    object_key,
                    ExtraArgs={
                        'ContentType': content_type,
                        'CacheControl': 'public, max-age=31536000'  # 1 year cache
                    }
                )
            
            # Generate public URL
            public_url = f"{self.public_url_base}/{object_key}"
            result['url'] = public_url
            result['success'] = True
            
            log.info(f"✅ Upload successful: {public_url}")
            
        except (BotoCoreError, ClientError) as e:
            error_msg = f"R2 upload failed: {str(e)}"
            log.error(error_msg)
            result['error'] = error_msg
            
        except Exception as e:
            error_msg = f"Upload failed: {str(e)}"
            log.error(error_msg)
            result['error'] = error_msg
            
        return result
    
    def _get_content_type(self, extension: str) -> str:
        """Get MIME type for file extension"""
        content_types = {
            '.jpg': 'image/jpeg',
            '.jpeg': 'image/jpeg', 
            '.png': 'image/png',
            '.gif': 'image/gif',
            '.webp': 'image/webp',
            '.bmp': 'image/bmp',
            '.svg': 'image/svg+xml',
            '.mp4': 'video/mp4',
            '.mov': 'video/quicktime',
            '.avi': 'video/x-msvideo',
            '.webm': 'video/webm',
            '.mkv': 'video/x-matroska',
            '.m4v': 'video/mp4'
        }
        return content_types.get(extension, 'application/octet-stream')


def log_upload_result(result: dict):
    """Append upload result to upload_log.jsonl"""
    try:
        with open(UPLOAD_LOG_PATH, 'a', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False)
            f.write('\n')
    except Exception as e:
        log.warning(f"Failed to log upload result: {e}")


def upload_single_file(file_path: str) -> dict:
    """Upload a single file and return result"""
    uploader = R2Uploader()
    result = uploader.upload_file(Path(file_path))
    log_upload_result(result)
    return result


def upload_directory(dir_path: str) -> list:
    """Upload all supported files from a directory"""
    directory = Path(dir_path)
    
    if not directory.exists():
        raise FileNotFoundError(f"Directory not found: {dir_path}")
        
    if not directory.is_dir():
        raise ValueError(f"Path is not a directory: {dir_path}")
    
    # Find all supported files
    supported_files = []
    for file_path in directory.iterdir():
        if file_path.is_file() and file_path.suffix.lower() in SUPPORTED_EXTENSIONS:
            supported_files.append(file_path)
    
    if not supported_files:
        log.warning(f"No supported media files found in {dir_path}")
        return []
    
    log.info(f"Found {len(supported_files)} files to upload from {dir_path}")
    
    # Upload each file
    uploader = R2Uploader()
    results = []
    
    for file_path in supported_files:
        result = uploader.upload_file(file_path)
        log_upload_result(result)
        results.append(result)
        
        # Small delay between uploads to be nice to R2
        time.sleep(0.1)
    
    return results


def main():
    parser = argparse.ArgumentParser(description='Upload media files to Cloudflare R2')
    
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('file', nargs='?', help='Single file to upload')
    group.add_argument('--dir', help='Directory containing files to batch upload')
    
    parser.add_argument('--json', action='store_true', 
                       help='Output results as JSON instead of human-readable format')
    
    args = parser.parse_args()
    
    try:
        if args.dir:
            # Batch upload from directory
            results = upload_directory(args.dir)
            
            if args.json:
                print(json.dumps(results, indent=2))
            else:
                successful = [r for r in results if r['success']]
                failed = [r for r in results if not r['success']]
                
                print(f"\n📊 Upload Summary:")
                print(f"   ✅ Successful: {len(successful)}")
                print(f"   ❌ Failed: {len(failed)}")
                
                if successful:
                    print("\n🔗 Successful uploads:")
                    for result in successful:
                        size_mb = result['size'] / (1024 * 1024)
                        print(f"   • {result['filename']} ({size_mb:.1f}MB) → {result['url']}")
                
                if failed:
                    print("\n❌ Failed uploads:")
                    for result in failed:
                        print(f"   • {result['filename']} - {result['error']}")
                        
        else:
            # Single file upload
            result = upload_single_file(args.file)
            
            if args.json:
                print(json.dumps(result, indent=2))
            else:
                if result['success']:
                    size_mb = result['size'] / (1024 * 1024)
                    print(f"✅ Upload successful!")
                    print(f"   File: {result['filename']} ({size_mb:.1f}MB)")
                    print(f"   URL: {result['url']}")
                else:
                    print(f"❌ Upload failed: {result['error']}")
                    sys.exit(1)
                    
    except KeyboardInterrupt:
        print("\n⚠️ Upload interrupted by user")
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