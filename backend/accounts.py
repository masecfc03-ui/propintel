"""
Accounts — subscription account storage for PropIntel.
Tracks Agent/Broker/Enterprise subscribers, plan status, monthly usage.
Uses same SQLite DB as orders.py (backend/data/orders.db).
"""
import os
import uuid
from datetime import datetime

try:
    import sqlite3
except ImportError:
    sqlite3 = None

DB_PATH = os.path.join(os.path.dirname(__file__), "data", "orders.db")

# Plan monthly report limits (None = unlimited)
PLAN_LIMITS = {
    "free":       0,
    "agent":      25,
    "broker":     None,
    "enterprise": None,
}

# Stripe live price ID → plan name
PRICE_TO_PLAN = {
    "price_1T4BZj35KKjpV0x2Nkwa7KOC": "agent",
    "price_1T4Bas35KKjpV0x296hS2Nm0":  "broker",
    "price_1T4Bat35KKjpV0x22E2oPyLU":  "enterprise",
}


def _get_conn():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    _ensure_schema(conn)
    return conn


def _ensure_schema(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS accounts (
            id                    TEXT PRIMARY KEY,
            email                 TEXT UNIQUE NOT NULL,
            stripe_customer_id    TEXT UNIQUE,
            stripe_subscription_id TEXT,
            plan                  TEXT NOT NULL DEFAULT 'free',
            status                TEXT NOT NULL DEFAULT 'inactive',
            reports_this_month    INTEGER NOT NULL DEFAULT 0,
            billing_month         TEXT,
            created_at            TEXT NOT NULL,
            updated_at            TEXT NOT NULL
        )
    """)
    conn.commit()


def _row_to_dict(row):
    return dict(row) if row else None


def _now():
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


def _this_month():
    return datetime.utcnow().strftime("%Y-%m")


def create_account(email, stripe_customer_id=None, plan="free", status="inactive"):
    """Create a new account. Returns account dict."""
    now = _now()
    account_id = str(uuid.uuid4())
    with _get_conn() as conn:
        try:
            conn.execute("""
                INSERT INTO accounts (id, email, stripe_customer_id, plan, status,
                                      reports_this_month, billing_month, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, 0, ?, ?, ?)
            """, (account_id, email.lower().strip(), stripe_customer_id, plan, status,
                  _this_month(), now, now))
            conn.commit()
        except sqlite3.IntegrityError:
            # Already exists — return existing
            row = conn.execute("SELECT * FROM accounts WHERE email = ?",
                               (email.lower().strip(),)).fetchone()
            return _row_to_dict(row)
    return get_account_by_email(email)


def get_account_by_email(email):
    """Return account dict or None."""
    with _get_conn() as conn:
        row = conn.execute("SELECT * FROM accounts WHERE email = ?",
                           (email.lower().strip(),)).fetchone()
    return _row_to_dict(row)


def get_account_by_customer_id(customer_id):
    """Return account dict or None."""
    with _get_conn() as conn:
        row = conn.execute("SELECT * FROM accounts WHERE stripe_customer_id = ?",
                           (customer_id,)).fetchone()
    return _row_to_dict(row)


def get_account_by_subscription_id(subscription_id):
    """Return account dict or None."""
    with _get_conn() as conn:
        row = conn.execute("SELECT * FROM accounts WHERE stripe_subscription_id = ?",
                           (subscription_id,)).fetchone()
    return _row_to_dict(row)


def update_account(account_id, **kwargs):
    """Update arbitrary fields on an account. Returns updated dict."""
    kwargs["updated_at"] = _now()
    fields = ", ".join("{} = ?".format(k) for k in kwargs)
    values = list(kwargs.values()) + [account_id]
    with _get_conn() as conn:
        conn.execute("UPDATE accounts SET {} WHERE id = ?".format(fields), values)
        conn.commit()
        row = conn.execute("SELECT * FROM accounts WHERE id = ?", (account_id,)).fetchone()
    return _row_to_dict(row)


def increment_usage(account_id):
    """
    Increment monthly report count. Auto-resets if month rolled over.
    Returns (new_count, limit) where limit is None for unlimited plans.
    """
    month = _this_month()
    with _get_conn() as conn:
        row = conn.execute("SELECT * FROM accounts WHERE id = ?", (account_id,)).fetchone()
        if not row:
            return (1, 0)
        account = _row_to_dict(row)
        current_month = account.get("billing_month") or month
        current_count = account.get("reports_this_month") or 0

        if current_month != month:
            # New billing month — reset counter
            new_count = 1
        else:
            new_count = current_count + 1

        conn.execute("""
            UPDATE accounts
            SET reports_this_month = ?, billing_month = ?, updated_at = ?
            WHERE id = ?
        """, (new_count, month, _now(), account_id))
        conn.commit()

    plan = account.get("plan", "free")
    limit = PLAN_LIMITS.get(plan, 0)
    return (new_count, limit)


def check_usage_limit(account_id):
    """
    Check if account can generate another report.
    Returns (allowed: bool, used: int, limit: int or None)
    """
    month = _this_month()
    with _get_conn() as conn:
        row = conn.execute("SELECT * FROM accounts WHERE id = ?", (account_id,)).fetchone()
        if not row:
            return (False, 0, 0)
        account = _row_to_dict(row)

    if account.get("status") not in ("active", "trialing"):
        return (False, 0, 0)

    plan = account.get("plan", "free")
    limit = PLAN_LIMITS.get(plan, 0)

    # Unlimited plans
    if limit is None:
        current = account.get("reports_this_month") or 0
        return (True, current, None)

    # Check if month rolled — count resets
    current_month = account.get("billing_month") or month
    current = account.get("reports_this_month") or 0
    if current_month != month:
        current = 0

    return (current < limit, current, limit)


def get_plan_from_price(price_id):
    """Map Stripe price ID to plan name."""
    return PRICE_TO_PLAN.get(price_id, "free")
