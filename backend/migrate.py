"""
Database Migration Script for PropIntel
Migrates from SQLite to PostgreSQL with automatic schema setup.
Safe to run multiple times (idempotent).
"""
import os
import logging
import time

# Setup logging
log = logging.getLogger("propintel.migrate")

def migrate():
    """
    Run database migrations. Creates all tables if they don't exist.
    Safe to run multiple times - uses IF NOT EXISTS clauses.
    
    This function:
    1. Detects database type from DATABASE_URL environment variable
    2. Creates all required tables (orders, leads)
    3. Handles schema migrations (adds new columns if needed)
    4. Is completely idempotent - safe to call on every app startup
    """
    DATABASE_URL = os.environ.get("DATABASE_URL", "")
    USE_PG = bool(DATABASE_URL)
    
    log.info("Starting database migration (database=%s)", "PostgreSQL" if USE_PG else "SQLite")
    
    try:
        if USE_PG:
            _migrate_postgres()
        else:
            _migrate_sqlite()
        
        log.info("Database migration completed successfully")
        
    except Exception as e:
        log.error("Database migration failed: %s", e, exc_info=True)
        raise


def _migrate_postgres():
    """Run PostgreSQL-specific migrations"""
    import psycopg2
    import psycopg2.extras
    
    DATABASE_URL = os.environ.get("DATABASE_URL")
    
    # Retry connection up to 3 times (Render Postgres can take a moment to be ready)
    for attempt in range(3):
        try:
            conn = psycopg2.connect(DATABASE_URL)
            break
        except psycopg2.OperationalError as e:
            if attempt == 2:  # Last attempt
                log.error("Failed to connect to PostgreSQL after 3 attempts: %s", e)
                raise
            log.warning("PostgreSQL connection attempt %d failed, retrying in 2s: %s", attempt + 1, e)
            time.sleep(2)
    
    try:
        cur = conn.cursor()
        
        # Create orders table
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
        
        # Create leads table
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
        
        # Migration: Add report_token column if it doesn't exist
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'orders' AND column_name = 'report_token'
        """)
        if not cur.fetchone():
            log.info("Adding report_token column to orders table")
            cur.execute("ALTER TABLE orders ADD COLUMN report_token TEXT")
        
        # Create indexes for better performance
        cur.execute("CREATE INDEX IF NOT EXISTS idx_orders_stripe_id ON orders(stripe_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_orders_token ON orders(report_token)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_leads_email ON leads(email)")
        
        conn.commit()
        log.info("PostgreSQL schema migration completed")
        
    finally:
        cur.close()
        conn.close()


def _migrate_sqlite():
    """Run SQLite-specific migrations (local development fallback)"""
    import sqlite3
    
    DB_PATH = os.path.join(os.path.dirname(__file__), "data", "orders.db")
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    
    try:
        # Create orders table
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
        
        # Create leads table
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
        
        # Migration: Add report_token column if it doesn't exist
        existing_cols = {row[1] for row in conn.execute("PRAGMA table_info(orders)").fetchall()}
        if "report_token" not in existing_cols:
            log.info("Adding report_token column to orders table")
            conn.execute("ALTER TABLE orders ADD COLUMN report_token TEXT")
        
        # Create indexes for better performance
        conn.execute("CREATE INDEX IF NOT EXISTS idx_orders_stripe_id ON orders(stripe_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_orders_token ON orders(report_token)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_leads_email ON leads(email)")
        
        conn.commit()
        log.info("SQLite schema migration completed")
        
    finally:
        conn.close()


if __name__ == "__main__":
    # Allow running migration script directly for testing
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S"
    )
    
    print("Running database migration...")
    migrate()
    print("Migration completed!")