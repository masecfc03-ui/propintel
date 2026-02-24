"""
Agents — storage for PropIntel agent profiles and branding.
Supports PostgreSQL (DATABASE_URL env var) with SQLite fallback.
"""
import os
import json
import uuid
import re
from datetime import datetime

DATABASE_URL = os.environ.get("DATABASE_URL", "")
_USE_PG = bool(DATABASE_URL)

if _USE_PG:
    import psycopg2
    import psycopg2.extras
else:
    import sqlite3

DB_PATH = os.path.join(os.path.dirname(__file__), "data", "agents.db")


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
    """SQLite schema setup"""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS agents (
            agent_id      TEXT PRIMARY KEY,
            name          TEXT NOT NULL,
            brokerage     TEXT,
            phone         TEXT,
            email         TEXT,
            photo_url     TEXT,
            accent_color  TEXT DEFAULT '#f59e0b',
            created_at    TEXT NOT NULL,
            report_count  INTEGER DEFAULT 0
        )
    """)
    conn.commit()


def _ensure_schema_pg(conn):
    """PostgreSQL schema setup"""
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS agents (
                agent_id      VARCHAR PRIMARY KEY,
                name          VARCHAR NOT NULL,
                brokerage     VARCHAR,
                phone         VARCHAR,
                email         VARCHAR,
                photo_url     VARCHAR,
                accent_color  VARCHAR DEFAULT '#f59e0b',
                created_at    TIMESTAMP WITH TIME ZONE NOT NULL,
                report_count  INTEGER DEFAULT 0
            )
        """)
    conn.commit()


def _generate_agent_id(name: str, brokerage: str = "") -> str:
    """
    Generate URL-safe agent ID from name and brokerage.
    Example: "Sarah Johnson" + "Compass" → "sarah-johnson-compass"
    """
    # Clean and combine name and brokerage
    parts = []
    if name:
        parts.append(name.strip())
    if brokerage:
        parts.append(brokerage.strip())
    
    combined = " ".join(parts).lower()
    
    # Convert to URL-safe slug
    # Remove non-alphanumeric characters except spaces and hyphens
    cleaned = re.sub(r'[^a-z0-9\s\-]', '', combined)
    # Replace spaces with hyphens
    slug = re.sub(r'\s+', '-', cleaned)
    # Remove multiple consecutive hyphens
    slug = re.sub(r'-+', '-', slug)
    # Remove leading/trailing hyphens
    slug = slug.strip('-')
    
    # Ensure minimum length and add suffix if needed
    if len(slug) < 3:
        slug = f"agent-{slug}-{str(uuid.uuid4())[:6]}"
    
    return slug


def create_agent(name: str, brokerage: str = "", phone: str = "", email: str = "", 
                photo_url: str = "", accent_color: str = "#f59e0b") -> dict:
    """
    Create new agent profile.
    Returns agent dict with generated agent_id.
    """
    if not name or not name.strip():
        raise ValueError("Agent name is required")
    
    # Validate accent color (simple hex check)
    if accent_color and not re.match(r'^#[0-9a-fA-F]{6}$', accent_color):
        accent_color = "#f59e0b"  # fallback to amber
    
    agent_id = _generate_agent_id(name, brokerage)
    now = datetime.utcnow().isoformat()
    
    agent = {
        "agent_id": agent_id,
        "name": name.strip(),
        "brokerage": brokerage.strip(),
        "phone": phone.strip(),
        "email": email.strip(),
        "photo_url": photo_url.strip(),
        "accent_color": accent_color,
        "created_at": now,
        "report_count": 0,
    }
    
    conn = _get_conn()
    try:
        if _USE_PG:
            with conn.cursor() as cur:
                # Check if agent_id already exists
                cur.execute("SELECT agent_id FROM agents WHERE agent_id = %s", (agent_id,))
                if cur.fetchone():
                    # Add random suffix to make unique
                    agent_id = f"{agent_id}-{str(uuid.uuid4())[:6]}"
                    agent["agent_id"] = agent_id
                
                cur.execute("""
                    INSERT INTO agents (agent_id, name, brokerage, phone, email, photo_url, accent_color, created_at, report_count)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (agent_id, agent["name"], agent["brokerage"], agent["phone"], 
                     agent["email"], agent["photo_url"], agent["accent_color"], now, 0))
        else:
            # Check if agent_id already exists
            existing = conn.execute("SELECT agent_id FROM agents WHERE agent_id = ?", (agent_id,)).fetchone()
            if existing:
                # Add random suffix to make unique
                agent_id = f"{agent_id}-{str(uuid.uuid4())[:6]}"
                agent["agent_id"] = agent_id
            
            conn.execute("""
                INSERT INTO agents (agent_id, name, brokerage, phone, email, photo_url, accent_color, created_at, report_count)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (agent_id, agent["name"], agent["brokerage"], agent["phone"], 
                 agent["email"], agent["photo_url"], agent["accent_color"], now, 0))
        
        conn.commit()
        return agent
    
    finally:
        conn.close()


def get_agent(agent_id: str) -> dict:
    """
    Fetch agent profile by agent_id.
    Returns agent dict or empty dict if not found.
    """
    if not agent_id:
        return {}
    
    conn = _get_conn()
    try:
        if _USE_PG:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute("SELECT * FROM agents WHERE agent_id = %s", (agent_id,))
                row = cur.fetchone()
        else:
            row = conn.execute("SELECT * FROM agents WHERE agent_id = ?", (agent_id,)).fetchone()
        
        return _row_to_dict(row) if row else {}
    
    finally:
        conn.close()


def increment_report_count(agent_id: str) -> bool:
    """
    Increment the report count for an agent.
    Returns True if successful, False if agent not found.
    """
    if not agent_id:
        return False
    
    conn = _get_conn()
    try:
        if _USE_PG:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE agents SET report_count = report_count + 1 
                    WHERE agent_id = %s
                """, (agent_id,))
                updated = cur.rowcount > 0
        else:
            cursor = conn.execute("""
                UPDATE agents SET report_count = report_count + 1 
                WHERE agent_id = ?
            """, (agent_id,))
            updated = cursor.rowcount > 0
        
        conn.commit()
        return updated
    
    finally:
        conn.close()


def list_agents(limit: int = 100) -> list:
    """
    List all agents, ordered by creation date (newest first).
    Returns list of agent dicts.
    """
    conn = _get_conn()
    try:
        if _USE_PG:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute("""
                    SELECT * FROM agents 
                    ORDER BY created_at DESC 
                    LIMIT %s
                """, (limit,))
                rows = cur.fetchall()
        else:
            rows = conn.execute("""
                SELECT * FROM agents 
                ORDER BY created_at DESC 
                LIMIT ?
            """, (limit,)).fetchall()
        
        return [_row_to_dict(row) for row in rows]
    
    finally:
        conn.close()


def get_agent_stats() -> dict:
    """
    Get aggregate stats for all agents.
    Returns dict with total agents, total reports, etc.
    """
    conn = _get_conn()
    try:
        if _USE_PG:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT 
                        COUNT(*) as total_agents,
                        SUM(report_count) as total_reports,
                        AVG(report_count) as avg_reports_per_agent
                    FROM agents
                """)
                row = cur.fetchone()
        else:
            row = conn.execute("""
                SELECT 
                    COUNT(*) as total_agents,
                    SUM(report_count) as total_reports,
                    AVG(report_count) as avg_reports_per_agent
                FROM agents
            """).fetchone()
        
        if row:
            return {
                "total_agents": row[0] or 0,
                "total_reports": row[1] or 0,
                "avg_reports_per_agent": round(row[2] or 0, 1),
            }
        else:
            return {"total_agents": 0, "total_reports": 0, "avg_reports_per_agent": 0}
    
    finally:
        conn.close()


# Migration function to add agents table to existing databases
def migrate_agents_table():
    """
    Ensure agents table exists in current database.
    Safe to run multiple times (idempotent).
    """
    conn = _get_conn()
    try:
        if _USE_PG:
            _ensure_schema_pg(conn)
        else:
            _ensure_schema(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    # Test the agent system
    print("Testing agent system...")
    
    # Create test agent
    test_agent = create_agent(
        name="Sarah Johnson",
        brokerage="Compass",
        phone="(214) 555-0100",
        email="sarah@compass.com",
        accent_color="#f59e0b"
    )
    print(f"Created agent: {test_agent}")
    
    # Fetch agent
    fetched = get_agent(test_agent["agent_id"])
    print(f"Fetched agent: {fetched}")
    
    # Increment report count
    success = increment_report_count(test_agent["agent_id"])
    print(f"Incremented report count: {success}")
    
    # Get updated agent
    updated = get_agent(test_agent["agent_id"])
    print(f"Updated agent: {updated}")
    
    # Get stats
    stats = get_agent_stats()
    print(f"Agent stats: {stats}")
    
    print("Agent system test completed!")