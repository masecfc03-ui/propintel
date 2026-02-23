"""
Orders — storage for PropIntel purchases.
Supports PostgreSQL (DATABASE_URL env var) with SQLite fallback.
"""
import os
import json
import uuid
from datetime import datetime

DATABASE_URL = os.environ.get("DATABASE_URL", "")
_USE_PG = bool(DATABASE_URL)

if _USE_PG:
    import psycopg2
    import psycopg2.extras
else:
    import sqlite3

DB_PATH = os.path.join(os.path.dirname(__file__), "data", "orders.db")


def _get_conn():
    if _USE_PG:
        conn = psycopg2.connect(DATABASE_URL)
        _ensure_schema_pg(conn)
        return conn
    else:
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        _ensure_schema(conn)
        return conn


def _row_to_dict(row):
    if _USE_PG:
        return dict(row) if row else {}
    return dict(row) if row else {}


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
            report_token TEXT,
            emailed     INTEGER DEFAULT 0,
            created_at  TEXT,
            updated_at  TEXT
        )
    """)
    # Safe idempotent migration — check column existence before ALTER
    existing_cols = {row[1] for row in conn.execute("PRAGMA table_info(orders)").fetchall()}
    if "report_token" not in existing_cols:
        conn.execute("ALTER TABLE orders ADD COLUMN report_token TEXT")
        conn.commit()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS leads (
            id          TEXT PRIMARY KEY,
            email       TEXT NOT NULL,
            address     TEXT,
            tier        TEXT,
            ip          TEXT,
            created_at  TEXT
        )
    """)
    conn.commit()


def _ensure_schema_pg(conn):
    cur = conn.cursor()
    cur.execute("""
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
            report_token TEXT,
            emailed     INTEGER DEFAULT 0,
            created_at  TEXT,
            updated_at  TEXT
        )
    """)
    # Safe idempotent migration — uses information_schema, no silent catch-all
    cur.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_name = 'orders' AND column_name = 'report_token'
    """)
    if not cur.fetchone():
        cur.execute("ALTER TABLE orders ADD COLUMN report_token TEXT")
        conn.commit()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS leads (
            id          TEXT PRIMARY KEY,
            email       TEXT NOT NULL,
            address     TEXT,
            tier        TEXT,
            ip          TEXT,
            created_at  TEXT
        )
    """)
    conn.commit()
    cur.close()


def _ph():
    """Return correct placeholder character for current DB."""
    return "%s" if _USE_PG else "?"


def create_order(stripe_id: str, tier: str, address: str,
                 customer_email: str = "", customer_name: str = "",
                 amount_cents: int = 0) -> dict:
    """Create a new order from a Stripe checkout.session.completed event."""
    order_id = str(uuid.uuid4())[:12]
    report_token = str(uuid.uuid4())  # full UUID for public access
    now = datetime.utcnow().isoformat()
    ph = _ph()

    conn = _get_conn()
    try:
        cur = conn.cursor() if _USE_PG else conn
        sql = f"""INSERT INTO orders
               (id, stripe_id, tier, address, customer_email, customer_name,
                amount_cents, status, report_token, created_at, updated_at)
               VALUES ({ph},{ph},{ph},{ph},{ph},{ph},{ph},'pending',{ph},{ph},{ph})"""
        cur.execute(sql, (order_id, stripe_id, tier, address, customer_email,
                          customer_name, amount_cents, report_token, now, now))
        conn.commit()
        if _USE_PG:
            cur.close()
    finally:
        conn.close()

    return get_order(order_id)


def create_lead(email: str, address: str = "", tier: str = "", ip: str = "") -> dict:
    """Store a free analyze lead (email capture)."""
    lead_id = str(uuid.uuid4())[:12]
    now = datetime.utcnow().isoformat()
    ph = _ph()

    conn = _get_conn()
    try:
        cur = conn.cursor() if _USE_PG else conn
        cur.execute(
            f"INSERT INTO leads (id, email, address, tier, ip, created_at) VALUES ({ph},{ph},{ph},{ph},{ph},{ph})",
            (lead_id, email, address, tier, ip, now)
        )
        conn.commit()
        if _USE_PG:
            cur.close()
    finally:
        conn.close()

    return {"id": lead_id, "email": email, "address": address, "created_at": now}


def update_order(order_id: str, **kwargs) -> dict:
    """Update order fields by order ID."""
    allowed = {"status", "report_json", "report_id", "report_token", "emailed", "address"}
    fields = {k: v for k, v in kwargs.items() if k in allowed}
    if not fields:
        return get_order(order_id)

    ph = _ph()
    fields["updated_at"] = datetime.utcnow().isoformat()
    set_clause = ", ".join(f"{k} = {ph}" for k in fields)
    values = list(fields.values()) + [order_id]

    conn = _get_conn()
    try:
        cur = conn.cursor() if _USE_PG else conn
        cur.execute(f"UPDATE orders SET {set_clause} WHERE id = {ph}", values)
        conn.commit()
        if _USE_PG:
            cur.close()
    finally:
        conn.close()

    return get_order(order_id)


def get_order(order_id: str) -> dict:
    ph = _ph()
    conn = _get_conn()
    try:
        if _USE_PG:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(f"SELECT * FROM orders WHERE id = {ph}", (order_id,))
            row = cur.fetchone()
            cur.close()
            return dict(row) if row else {}
        else:
            row = conn.execute(f"SELECT * FROM orders WHERE id = {ph}", (order_id,)).fetchone()
            return dict(row) if row else {}
    finally:
        conn.close()


def get_order_by_token(token: str) -> dict:
    """Look up an order by its public report_token (for customer-facing access)."""
    ph = _ph()
    conn = _get_conn()
    try:
        if _USE_PG:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(f"SELECT * FROM orders WHERE report_token = {ph}", (token,))
            row = cur.fetchone()
            cur.close()
            return dict(row) if row else {}
        else:
            row = conn.execute(f"SELECT * FROM orders WHERE report_token = {ph}", (token,)).fetchone()
            return dict(row) if row else {}
    finally:
        conn.close()


def get_order_by_stripe(stripe_id: str) -> dict:
    ph = _ph()
    conn = _get_conn()
    try:
        if _USE_PG:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(f"SELECT * FROM orders WHERE stripe_id = {ph}", (stripe_id,))
            row = cur.fetchone()
            cur.close()
            return dict(row) if row else {}
        else:
            row = conn.execute(f"SELECT * FROM orders WHERE stripe_id = {ph}", (stripe_id,)).fetchone()
            return dict(row) if row else {}
    finally:
        conn.close()


def list_orders(limit: int = 100) -> list:
    ph = _ph()
    conn = _get_conn()
    try:
        if _USE_PG:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(f"SELECT * FROM orders ORDER BY created_at DESC LIMIT {ph}", (limit,))
            rows = cur.fetchall()
            cur.close()
            return [dict(r) for r in rows]
        else:
            rows = conn.execute(
                f"SELECT * FROM orders ORDER BY created_at DESC LIMIT {ph}", (limit,)
            ).fetchall()
            return [dict(r) for r in rows]
    finally:
        conn.close()


def list_leads(limit: int = 500) -> list:
    ph = _ph()
    conn = _get_conn()
    try:
        if _USE_PG:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(f"SELECT * FROM leads ORDER BY created_at DESC LIMIT {ph}", (limit,))
            rows = cur.fetchall()
            cur.close()
            return [dict(r) for r in rows]
        else:
            rows = conn.execute(
                f"SELECT * FROM leads ORDER BY created_at DESC LIMIT {ph}", (limit,)
            ).fetchall()
            return [dict(r) for r in rows]
    finally:
        conn.close()


def stats() -> dict:
    conn = _get_conn()
    try:
        if _USE_PG:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM orders"); total = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM orders WHERE status='complete'"); completed = cur.fetchone()[0]
            cur.execute("SELECT COALESCE(SUM(amount_cents),0) FROM orders WHERE status='complete'"); revenue_cents = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM orders WHERE tier='starter' AND status='complete'"); starter = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM orders WHERE tier='pro' AND status='complete'"); pro = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM leads"); leads = cur.fetchone()[0]
            cur.close()
        else:
            total = conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
            completed = conn.execute("SELECT COUNT(*) FROM orders WHERE status='complete'").fetchone()[0]
            revenue_cents = conn.execute("SELECT SUM(amount_cents) FROM orders WHERE status='complete'").fetchone()[0] or 0
            starter = conn.execute("SELECT COUNT(*) FROM orders WHERE tier='starter' AND status='complete'").fetchone()[0]
            pro = conn.execute("SELECT COUNT(*) FROM orders WHERE tier='pro' AND status='complete'").fetchone()[0]
            leads = conn.execute("SELECT COUNT(*) FROM leads").fetchone()[0]
        return {
            "total_orders": total,
            "completed_orders": completed,
            "revenue_dollars": round(revenue_cents / 100, 2),
            "starter_count": starter,
            "pro_count": pro,
            "total_leads": leads,
        }
    finally:
        conn.close()
