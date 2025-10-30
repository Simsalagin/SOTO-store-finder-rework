"""Database models and schema definitions."""

STORES_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS stores (
    market_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    street TEXT,
    zip TEXT,
    city TEXT,
    latitude REAL,
    longitude REAL,
    phone TEXT,
    email TEXT,
    country_code TEXT,
    status TEXT,
    opening_day DATE,
    is_loyalty_market BOOLEAN,
    google_maps_link TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

OPENING_HOURS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS opening_hours (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    market_id INTEGER NOT NULL,
    weekday TEXT NOT NULL,
    open_from TEXT,
    open_until TEXT,
    FOREIGN KEY (market_id) REFERENCES stores (market_id) ON DELETE CASCADE
)
"""

CREATE_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_market_id ON opening_hours(market_id)
"""
