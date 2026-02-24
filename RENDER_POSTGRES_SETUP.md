# Render PostgreSQL Setup Guide

This guide walks through adding PostgreSQL to your PropIntel deployment on Render.

## Why PostgreSQL?

- **Reliability**: PostgreSQL is production-grade with ACID compliance
- **Performance**: Better for concurrent users and larger datasets
- **Features**: Advanced querying, indexing, and data integrity
- **Scalability**: Handles growth better than SQLite file-based storage

## Before You Start

- Your app already has **SQLite fallback** - if PostgreSQL isn't available, it automatically falls back to SQLite
- **psycopg2-binary** is already in requirements.txt
- Migration scripts run automatically on app startup
- **Zero code changes needed** - just add the database addon

## Step-by-Step Setup

### 1. Add PostgreSQL Addon

1. **Login to Render** → Go to your Dashboard
2. **Find your PropIntel service** → Click on it
3. **Go to Settings tab** → Scroll down to "Add-ons"
4. **Click "Add Database"** → Select **PostgreSQL**
5. **Choose plan**:
   - **Starter ($7/month)**: Good for development/testing
   - **Standard ($20/month)**: Recommended for production
6. **Click "Create Database"**

### 2. Environment Variable (Automatic)

Render **automatically sets** the `DATABASE_URL` environment variable when you add PostgreSQL.

**You don't need to do anything** - Render connects your service to the database automatically.

**Format**: `postgresql://username:password@host:port/database`

### 3. Trigger Redeploy

After adding PostgreSQL:

1. **Go to Deploys tab** on your service
2. **Click "Manual Deploy"** → **Deploy latest commit**
3. **Watch the logs** - you should see:
   ```
   [INFO] Running database migrations...
   [INFO] Starting database migration (database=PostgreSQL)
   [INFO] PostgreSQL schema migration completed
   [INFO] Database migration completed successfully
   ```

### 4. Verify PostgreSQL is Active

**Check health endpoint**:
```bash
curl https://your-app.onrender.com/api/health
```

Look for:
```json
{
  "status": "ok",
  "config_complete": true,
  "database": "PostgreSQL"
}
```

**Check app logs**:
- ✅ `database=PostgreSQL` in migration logs
- ✅ No SQLite fallback warnings
- ✅ Database operations working

## Migration Details

### What Happens Automatically

1. **On app startup**: Migration script runs
2. **Schema creation**: All tables created if they don't exist
3. **Safe migrations**: Existing data preserved
4. **Indexes added**: For better query performance
5. **Column migrations**: New columns added safely

### Tables Created

- **orders**: Stripe payments, report generation, customer data
- **leads**: Email captures, free analysis requests

### No Data Loss

- **Safe migrations**: Uses `IF NOT EXISTS` clauses
- **Idempotent**: Can run multiple times safely
- **Preserves data**: Existing records untouched

## Local Development

Your local development environment continues to use **SQLite** (unless you set a local `DATABASE_URL`).

**This is intentional** - keeps local development simple while production uses PostgreSQL.

## Troubleshooting

### Migration Fails

**Check logs for**:
```
[ERROR] Database migration failed: FATAL: password authentication failed
```

**Solution**: Database addon might be provisioning. Wait 2-3 minutes and redeploy.

### App Won't Start

**Check environment variables**:
1. Go to Settings → Environment
2. Verify `DATABASE_URL` exists and looks like: `postgresql://...`
3. If missing, remove and re-add the PostgreSQL addon

### Performance Issues

**Check connection count**:
- Starter plan: 20 connections max
- Standard plan: 100 connections max
- Consider upgrading if you hit limits

### Fallback to SQLite

If PostgreSQL fails, the app **automatically falls back to SQLite** and logs:
```
[WARNING] PostgreSQL connection failed, falling back to SQLite
```

**This is safe** - your app stays online while you fix database issues.

## Cost

- **PostgreSQL Starter**: $7/month
- **PostgreSQL Standard**: $20/month  
- **Free alternative**: SQLite fallback (built-in)

## Security

- **Automatic backups**: Render handles this
- **SSL connections**: Enabled by default
- **Network isolation**: Database not publicly accessible
- **Connection pooling**: Built into Render PostgreSQL

## Next Steps

1. ✅ **Add PostgreSQL addon** (5 minutes)
2. ✅ **Redeploy app** (5 minutes)  
3. ✅ **Verify health endpoint** (1 minute)
4. 🎉 **Production PostgreSQL ready!**

---

**Questions?** Check Render docs: https://render.com/docs/databases

**PropIntel specific issues?** The app logs will tell you exactly what's happening with database connections.