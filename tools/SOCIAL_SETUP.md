# Social Media Tools Setup Guide

This guide walks you through setting up the R2 upload and Buffer posting tools for automating social media content from PropIntel property reports.

## Overview

Two main tools:
- **`r2_upload.py`** - Upload screenshots/videos to Cloudflare R2 storage
- **`buffer_post.py`** - Queue posts to social media via Buffer

## 🪣 Cloudflare R2 Setup (Free Tier: 10GB)

### 1. Create R2 Account

1. Go to [Cloudflare Dashboard](https://dash.cloudflare.com)
2. Sign up for free account (if needed)
3. Navigate to **R2 Object Storage** in the sidebar
4. Click **"Create bucket"** and choose a unique name (e.g., `propintel-media`)

### 2. Get API Credentials

1. In Cloudflare dashboard, go to **"My Profile"** > **"API Tokens"**
2. Click **"Create Token"** > **"Custom token"**
3. Configure permissions:
   - **Account**: Your account
   - **Zone Resources**: `Include - All zones`
   - **Account Resources**: `Include - All accounts`
   - **Permissions**: 
     - `Account:Cloudflare R2:Edit`
     - `Zone:Zone Settings:Read`
4. Click **"Continue to Summary"** > **"Create Token"**
5. Copy the token (this is your `CF_R2_ACCESS_KEY`)

### 3. Get Account ID & Secret Key

1. In R2 dashboard, click **"Manage R2 API tokens"**
2. Copy your **Account ID** (this is `CF_R2_ACCOUNT_ID`)
3. Click **"Create API token"** if you need a new one
4. For existing tokens, click **"View"** to see the **Secret Key**

### 4. Enable Public Access (for social media URLs)

1. Go to your R2 bucket settings
2. Click **"Settings"** tab
3. Under **"Public Access"**, click **"Allow Access"**
4. Your public URL will be: `https://pub-{bucket-name}.r2.dev`

## 📱 Buffer API Setup (Free Plan: 3 Channels)

### 1. Create Buffer Account

1. Go to [Buffer.com](https://buffer.com) and sign up for free
2. Connect your social media accounts:
   - Instagram Business account
   - Twitter account  
   - TikTok Business account (if available)
   - Facebook Page, LinkedIn, etc.

### 2. Create API Access Token

1. Go to [Buffer Developers](https://buffer.com/developers) 
2. Sign in with your Buffer account
3. Click **"Create an App"** or **"Access Token"**
4. If creating an app:
   - Name: "PropIntel Social Automation"
   - Description: "Automated social media posting for property reports"
   - Website: Your PropIntel domain
5. Generate an **Access Token** 
6. Copy the token (this is your `BUFFER_ACCESS_TOKEN`)

### Alternative: Manual Token Generation

1. Go to [Buffer Developers Apps](https://buffer.com/developers/apps)
2. Click your app name
3. In the **"Access Token"** section, click **"Create Access Token"**
4. Copy the generated token

## 🔧 Environment Variables Setup

Create a `.env` file in your project root or set these environment variables:

```bash
# Cloudflare R2 Configuration
CF_R2_ACCOUNT_ID=your_cloudflare_account_id
CF_R2_ACCESS_KEY=your_r2_access_key_id
CF_R2_SECRET_KEY=your_r2_secret_access_key  
CF_R2_BUCKET=your_bucket_name

# Buffer API Configuration
BUFFER_ACCESS_TOKEN=your_buffer_access_token
```

### Setting Environment Variables

**Option 1: .env file (Recommended)**
```bash
# Create .env file in your project root
echo "CF_R2_ACCOUNT_ID=abc123..." >> .env
echo "CF_R2_ACCESS_KEY=def456..." >> .env  
echo "CF_R2_SECRET_KEY=ghi789..." >> .env
echo "CF_R2_BUCKET=propintel-media" >> .env
echo "BUFFER_ACCESS_TOKEN=xyz789..." >> .env
```

**Option 2: Export variables (Session-based)**
```bash
export CF_R2_ACCOUNT_ID="your_account_id"
export CF_R2_ACCESS_KEY="your_access_key"
export CF_R2_SECRET_KEY="your_secret_key" 
export CF_R2_BUCKET="your_bucket_name"
export BUFFER_ACCESS_TOKEN="your_buffer_token"
```

**Option 3: System environment (Persistent)**
Add to your `~/.bashrc` or `~/.zshrc`:
```bash
export CF_R2_ACCOUNT_ID="your_account_id"
# ... other variables
```

## 📦 Dependencies Installation

Install required Python packages:

```bash
# Navigate to tools directory
cd /path/to/propintel/tools/

# Install dependencies  
pip install boto3 requests python-dotenv

# Or create a requirements.txt:
pip freeze > requirements.txt
```

## 🧪 Testing the Setup

### Test R2 Upload

```bash
# Test with a sample image
python3 r2_upload.py /path/to/test-image.jpg

# Test batch upload
python3 r2_upload.py --dir /path/to/screenshots/

# Check upload log
cat upload_log.jsonl
```

### Test Buffer Connection

```bash
# List connected Buffer profiles
python3 buffer_post.py --list-services

# Test post (replace with actual URL)
python3 buffer_post.py "https://pub-yourBucket.r2.dev/media/123_test.jpg" "Test post from PropIntel! 🏠" --platforms instagram twitter
```

## 🚀 Usage Examples

### Complete Workflow

```bash
# 1. Upload property screenshots to R2
python3 r2_upload.py --dir ./property_screenshots/ --json > upload_results.json

# 2. Extract URLs and post to social media
python3 buffer_post.py "https://pub-bucket.r2.dev/media/123_property.jpg" "🏠 New listing: 3BR/2BA ranch in Dallas! Priced to sell at $425K. DM for details! #RealEstate #Dallas #NewListing" --platforms instagram twitter tiktok

# 3. Check upload logs
tail -f upload_log.jsonl
```

### Automation Script Example

```bash
#!/bin/bash
# automated_social_post.sh

# Upload screenshots
RESULTS=$(python3 r2_upload.py --dir ./reports/screenshots/ --json)
IMAGE_URL=$(echo "$RESULTS" | jq -r '.[0].url')

# Post to social media if upload successful
if [ "$?" -eq 0 ] && [ "$IMAGE_URL" != "null" ]; then
    python3 buffer_post.py "$IMAGE_URL" "🏠 Fresh market analysis ready! Check out this property report. #RealEstate #PropertyInvestment #MarketData" --platforms instagram twitter
    echo "✅ Social media post queued successfully"
else
    echo "❌ Upload failed, skipping social media post"
fi
```

## 📝 Cost Information

### Cloudflare R2 (Free Tier Limits)
- **Storage**: 10 GB/month free
- **Class A Operations**: 1M/month free (uploads, lists)
- **Class B Operations**: 10M/month free (downloads)
- **Egress**: 10 GB/month free

Typical usage for property reports:
- ~100 reports/month × 5 images/report × 2MB/image = 1GB storage
- Well within free tier limits

### Buffer (Free Plan Limits)
- **Connected Accounts**: 3 social accounts
- **Scheduled Posts**: 10 posts per account per day
- **Analytics**: Basic metrics

For higher volume, upgrade to Buffer Pro ($5/month) for:
- Unlimited posts
- 8+ social accounts  
- Advanced analytics

## 🔍 Troubleshooting

### Common Issues

**R2 Upload Fails**
- Check bucket name and R2 credentials
- Verify bucket has public access enabled
- Ensure file extensions are supported (jpg, png, mp4, etc.)

**Buffer Post Fails**
- Verify Buffer token is valid and not expired
- Check if social accounts are still connected
- Ensure caption length meets platform requirements

**"No profiles found" Error**
- Run `python3 buffer_post.py --list-services` to see connected accounts
- Reconnect social accounts in Buffer dashboard if needed
- Check platform names match exactly (instagram, twitter, tiktok)

### Debug Mode

Enable debug logging:
```bash
export PYTHONPATH="${PYTHONPATH}:."
python3 -m logging.basicConfig level=DEBUG r2_upload.py
```

### Support

- **R2 Issues**: [Cloudflare Community](https://community.cloudflare.com/)
- **Buffer Issues**: [Buffer Help Center](https://help.buffer.com/)
- **PropIntel Issues**: Create GitHub issue on repository

## 🔒 Security Notes

- Never commit `.env` files to version control
- Rotate API tokens regularly (every 90 days recommended)  
- Use environment-specific tokens (dev/staging/prod)
- Monitor API usage to detect unauthorized access
- Consider using IAM roles instead of access keys in production

## 📊 Monitoring & Analytics

### Upload Monitoring
```bash
# View recent uploads
tail -10 upload_log.jsonl | jq .

# Count uploads by date
cat upload_log.jsonl | jq -r '.uploaded_at[:10]' | sort | uniq -c

# Calculate total storage used
cat upload_log.jsonl | jq -r 'select(.success==true) | .size' | awk '{sum+=$1} END {print sum/1024/1024 " MB"}'
```

### Buffer Analytics
Use Buffer's web dashboard for:
- Post performance metrics
- Engagement rates
- Optimal posting times
- Audience insights