# Walk Score API Integration Setup

## Overview
PropIntel now includes Walk Score, Transit Score, and Bike Score for all property reports.

## Free API Registration Required

1. **Register at**: https://www.walkscore.com/professional/api-sign-up.php
2. **Use these details**:
   - **Email**: masecfc03@gmail.com
   - **Website**: propertyvalueintel.com  
   - **Description**: "Property intelligence reports for real estate agents"
   - **Plan**: Free Version (5,000 calls/day)

3. **Get your API key** immediately after form submission

## Setup Instructions

### 1. Add API Key to Local Environment
```bash
# In backend/.env, add:
WALKSCORE_API_KEY=your_actual_api_key_here
```

### 2. Add API Key to Render (Production)
```bash
curl -X POST "https://api.render.com/v1/services/srv-d6dvsn7gi27c738nlfp0/env-vars" \
  -H "Authorization: Bearer rnd_RLLYc5yQZeAcihgdDkp4TMEIkQzS" \
  -H "Content-Type: application/json" \
  -d '[{"key":"WALKSCORE_API_KEY","value":"your_actual_api_key_here"}]'
```

### 3. Test Integration
```bash
cd backend
python3 test_walkscore_integration.py
```

## How It Works

- **Automatic**: Runs for every report when `lat`, `lng`, and `address` are available
- **Graceful fallback**: If API key is missing or API fails, reports show "Not available" instead of breaking
- **Data returned**:
  - Walk Score (0-100) + description (e.g. "Very Walkable")
  - Transit Score (0-100) + description (e.g. "Good Transit")  
  - Bike Score (0-100) + description (e.g. "Bikeable")

## Integration Status

✅ **Already integrated** - No code changes needed:
- `scrapers/walkscore.py` - API client implementation
- `pipeline.py` - Auto-runs for every report  
- Error handling and fallbacks implemented

⏳ **Needs manual setup**:
- Register for API key (browser automation failed)
- Add WALKSCORE_API_KEY to local .env
- Add WALKSCORE_API_KEY to Render env vars

## API Limits
- **Free tier**: 5,000 calls/day
- **Coverage**: United States and Canada
- **No rate limits** beyond daily quota