"""
Request cache — prevents re-hitting paid APIs for the same address.

Strategy:
  - Key: sha256(address_normalized + tier)
  - TTL: 24h for full reports, 1h for stale-ok data
  - Backend: SQLite (zero-dep, works on free Render), upgrades to Redis if needed
  - On cache hit: return JSON, log "cache hit"
  - On cache miss: run pipeline, store result, return

Rules (from LEARNINGS.md):
  - Every cache hit masks stale data → explicit TTL, not permanent
  - Every cache invalidation ruins your week → simple key structure, easy to purge
"""

import hashlib
import json
import sqlite3
import time
import os
import logging

log = logging.getLogger(__name__)

CACHE_DB   = os.path.join(os.path.dirname(__file__), "reports_cache", "cache.db")
DEFAULT_TTL = 60 * 60 * 24   # 24 hours
SHORT_TTL   = 60 * 60 * 1    # 1 hour (for development / volatile data)


def _db():
    os.makedirs(os.path.dirname(CACHE_DB), exist_ok=True)
    conn = sqlite3.connect(CACHE_DB, timeout=10)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS report_cache (
            cache_key   TEXT PRIMARY KEY,
            address     TEXT,
            tier        TEXT,
            result_json TEXT NOT NULL,
            created_at  INTEGER NOT NULL,
            expires_at  INTEGER NOT NULL,
            hit_count   INTEGER DEFAULT 0
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_expires ON report_cache(expires_at)")
    conn.commit()
    return conn


def _make_key(address: str, tier: str) -> str:
    """Normalized cache key — strips whitespace/case so '3229 Forest Ln' == '3229 forest ln'."""
    raw = f"{address.strip().lower()}|{tier.strip().lower()}"
    return hashlib.sha256(raw.encode()).hexdigest()


def get(address: str, tier: str):
    """Return cached report or None if miss/expired."""
    key = _make_key(address, tier)
    now = int(time.time())
    try:
        conn = _db()
        row = conn.execute(
            "SELECT result_json, expires_at, hit_count FROM report_cache WHERE cache_key = ?",
            (key,)
        ).fetchone()

        if not row:
            log.debug("Cache miss: %s [%s]", address[:40], tier)
            return None

        result_json, expires_at, hit_count = row

        if now > expires_at:
            log.info("Cache expired: %s [%s] — will re-fetch", address[:40], tier)
            conn.execute("DELETE FROM report_cache WHERE cache_key = ?", (key,))
            conn.commit()
            return None

        # Update hit counter
        conn.execute(
            "UPDATE report_cache SET hit_count = ? WHERE cache_key = ?",
            (hit_count + 1, key)
        )
        conn.commit()

        log.info("Cache HIT: %s [%s] (hit #%d, expires in %dm)",
                 address[:40], tier, hit_count + 1,
                 (expires_at - now) // 60)

        result = json.loads(result_json)
        result["_cached"] = True
        result["_cache_age_min"] = (now - (expires_at - DEFAULT_TTL)) // 60
        return result

    except Exception as e:
        log.warning("Cache read error (bypassing): %s", e)
        return None   # Never block on cache failure


def set(address: str, tier: str, result: dict, ttl: int = DEFAULT_TTL) -> None:
    """Store report in cache. Silently skips on any error."""
    if result.get("error"):
        log.debug("Not caching error result for %s", address[:40])
        return   # Don't cache errors

    key = _make_key(address, tier)
    now = int(time.time())
    try:
        conn = _db()
        conn.execute("""
            INSERT OR REPLACE INTO report_cache
              (cache_key, address, tier, result_json, created_at, expires_at, hit_count)
            VALUES (?, ?, ?, ?, ?, ?, 0)
        """, (key, address, tier, json.dumps(result), now, now + ttl))
        conn.commit()
        log.info("Cached: %s [%s] TTL=%dh", address[:40], tier, ttl // 3600)
    except Exception as e:
        log.warning("Cache write error (continuing): %s", e)


def invalidate(address: str, tier: str = None) -> int:
    """Remove cached entries for an address. Returns rows deleted."""
    try:
        conn = _db()
        if tier:
            key = _make_key(address, tier)
            n = conn.execute("DELETE FROM report_cache WHERE cache_key = ?", (key,)).rowcount
        else:
            # Invalidate all tiers for this address
            pattern = address.strip().lower()
            # Must check by address field since key is hashed
            n = conn.execute(
                "DELETE FROM report_cache WHERE LOWER(address) = ?", (pattern,)
            ).rowcount
        conn.commit()
        log.info("Invalidated %d cache entries for: %s", n, address[:40])
        return n
    except Exception as e:
        log.warning("Cache invalidate error: %s", e)
        return 0


def purge_expired() -> int:
    """Delete all expired entries. Run periodically."""
    try:
        conn = _db()
        n = conn.execute(
            "DELETE FROM report_cache WHERE expires_at < ?", (int(time.time()),)
        ).rowcount
        conn.commit()
        log.info("Purged %d expired cache entries", n)
        return n
    except Exception as e:
        log.warning("Cache purge error: %s", e)
        return 0


def stats() -> dict:
    """Return cache statistics for /api/health."""
    try:
        conn = _db()
        total = conn.execute("SELECT COUNT(*) FROM report_cache").fetchone()[0]
        live  = conn.execute(
            "SELECT COUNT(*) FROM report_cache WHERE expires_at > ?", (int(time.time()),)
        ).fetchone()[0]
        hits  = conn.execute("SELECT SUM(hit_count) FROM report_cache").fetchone()[0] or 0
        return {"total": total, "live": live, "total_hits": hits}
    except Exception:
        return {"total": 0, "live": 0, "total_hits": 0}
