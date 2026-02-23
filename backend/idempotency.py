"""
Webhook idempotency — prevents double-processing of Stripe events.

Rule: Every webhook fires twice when you least expect it.
Solution: Store processed event IDs. Skip if already seen.

Storage: Same orders.db (SQLite / Postgres), separate table.
Retention: 72 hours (Stripe retries for 72h max).
"""

import logging
import time
import os

log = logging.getLogger(__name__)

_TABLE_CREATED = False


def _ensure_table(conn):
    global _TABLE_CREATED
    if _TABLE_CREATED:
        return
    conn.execute("""
        CREATE TABLE IF NOT EXISTS processed_events (
            event_id    TEXT PRIMARY KEY,
            event_type  TEXT,
            processed_at INTEGER NOT NULL,
            result      TEXT
        )
    """)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_event_processed_at ON processed_events(processed_at)"
    )
    _TABLE_CREATED = True


def already_processed(conn, event_id: str) -> bool:
    """Return True if this Stripe event was already handled."""
    try:
        _ensure_table(conn)
        row = conn.execute(
            "SELECT event_id FROM processed_events WHERE event_id = ?",
            (event_id,)
        ).fetchone()
        if row:
            log.warning("Duplicate webhook ignored: %s (already processed)", event_id)
            return True
        return False
    except Exception as e:
        log.error("Idempotency check failed: %s — allowing event through", e)
        return False   # Fail open: better to process twice than never


def mark_processed(conn, event_id: str, event_type: str, result: str = "ok") -> None:
    """Record that this event has been handled."""
    try:
        _ensure_table(conn)
        conn.execute("""
            INSERT OR IGNORE INTO processed_events (event_id, event_type, processed_at, result)
            VALUES (?, ?, ?, ?)
        """, (event_id, event_type, int(time.time()), result))
        log.info("Event marked processed: %s (%s)", event_id, event_type)
    except Exception as e:
        log.error("Failed to mark event processed: %s", e)
        # Non-fatal — log and continue


def purge_old_events(conn, max_age_hours: int = 96) -> int:
    """Remove events older than max_age_hours. Call from a maintenance routine."""
    try:
        _ensure_table(conn)
        cutoff = int(time.time()) - (max_age_hours * 3600)
        n = conn.execute(
            "DELETE FROM processed_events WHERE processed_at < ?", (cutoff,)
        ).rowcount
        if n:
            log.info("Purged %d old processed events (>%dh)", n, max_age_hours)
        return n
    except Exception as e:
        log.warning("Event purge failed: %s", e)
        return 0
