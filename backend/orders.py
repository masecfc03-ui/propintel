"""
Orders — lightweight SQLite storage for PropIntel purchases.
Tracks Stripe payments, report status, and delivery.
"""
import os
import json
import sqlite3
import uuid
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(__file__), "data", "orders.db")


def _get_conn():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    _ensure_schema(conn)
    return conn


def _ensure_schema(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            id          TEXT PRIMARY KEY,
            stripe_id   TEXT UNIQUE,
            tier        TEXT NOT NULL,
            address     TEXT,
            customer_email TEXT,
            customer_name  TEXT,
            amount_cents   INTEGER,
            status      TEXT DEFAULT 'pending',
            report_json TEXT,
            report_id   TEXT,
            emailed     INTEGER DEFAULT 0,
            created_at  TEXT,
            updated_at  TEXT
        )
    """)
    conn.commit()


def create_order(stripe_id: str, tier: str, address: str,
                 customer_email: str = "", customer_name: str = "",
                 amount_cents: int = 0) -> dict:
    """Create a new order from a Stripe checkout.session.completed event."""
    order_id = str(uuid.uuid4())[:12]
    now = datetime.utcnow().isoformat()

    conn = _get_conn()
    try:
        conn.execute(
            """INSERT INTO orders
               (id, stripe_id, tier, address, customer_email, customer_name,
                amount_cents, status, created_at, updated_at)
               VALUES (?,?,?,?,?,?,?,'pending',?,?)""",
            (order_id, stripe_id, tier, address, customer_email, customer_name,
             amount_cents, now, now)
        )
        conn.commit()
    finally:
        conn.close()

    return get_order(order_id)


def update_order(order_id: str, **kwargs) -> dict:
    """Update order fields by order ID."""
    allowed = {"status", "report_json", "report_id", "emailed", "address"}
    fields = {k: v for k, v in kwargs.items() if k in allowed}
    if not fields:
        return get_order(order_id)

    fields["updated_at"] = datetime.utcnow().isoformat()
    set_clause = ", ".join(f"{k} = ?" for k in fields)
    values = list(fields.values()) + [order_id]

    conn = _get_conn()
    try:
        conn.execute(f"UPDATE orders SET {set_clause} WHERE id = ?", values)
        conn.commit()
    finally:
        conn.close()

    return get_order(order_id)


def get_order(order_id: str) -> dict:
    conn = _get_conn()
    try:
        row = conn.execute("SELECT * FROM orders WHERE id = ?", (order_id,)).fetchone()
        return dict(row) if row else {}
    finally:
        conn.close()


def get_order_by_stripe(stripe_id: str) -> dict:
    conn = _get_conn()
    try:
        row = conn.execute("SELECT * FROM orders WHERE stripe_id = ?", (stripe_id,)).fetchone()
        return dict(row) if row else {}
    finally:
        conn.close()


def list_orders(limit: int = 100) -> list:
    conn = _get_conn()
    try:
        rows = conn.execute(
            "SELECT * FROM orders ORDER BY created_at DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def stats() -> dict:
    conn = _get_conn()
    try:
        total = conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
        completed = conn.execute("SELECT COUNT(*) FROM orders WHERE status='complete'").fetchone()[0]
        revenue_cents = conn.execute("SELECT SUM(amount_cents) FROM orders WHERE status='complete'").fetchone()[0] or 0
        starter = conn.execute("SELECT COUNT(*) FROM orders WHERE tier='starter' AND status='complete'").fetchone()[0]
        pro = conn.execute("SELECT COUNT(*) FROM orders WHERE tier='pro' AND status='complete'").fetchone()[0]
        return {
            "total_orders": total,
            "completed_orders": completed,
            "revenue_dollars": round(revenue_cents / 100, 2),
            "starter_count": starter,
            "pro_count": pro,
        }
    finally:
        conn.close()
