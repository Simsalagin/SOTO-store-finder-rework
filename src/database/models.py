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
    osm_latitude REAL,
    osm_longitude REAL,
    osm_checked BOOLEAN DEFAULT 0,
    osm_checked_at TIMESTAMP,
    osm_display_name TEXT,
    final_latitude REAL,
    final_longitude REAL,
    geocoding_source TEXT,
    geocoding_confidence TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

# Migration SQL for adding OSM columns to existing databases
OSM_MIGRATION_SQL = [
    "ALTER TABLE stores ADD COLUMN osm_latitude REAL",
    "ALTER TABLE stores ADD COLUMN osm_longitude REAL",
    "ALTER TABLE stores ADD COLUMN osm_checked BOOLEAN DEFAULT 0",
    "ALTER TABLE stores ADD COLUMN osm_checked_at TIMESTAMP",
    "ALTER TABLE stores ADD COLUMN osm_display_name TEXT"
]

# Migration SQL for adding final coordinate columns for Leaflet
FINAL_COORDS_MIGRATION_SQL = [
    "ALTER TABLE stores ADD COLUMN final_latitude REAL",
    "ALTER TABLE stores ADD COLUMN final_longitude REAL",
    "ALTER TABLE stores ADD COLUMN geocoding_source TEXT",
    "ALTER TABLE stores ADD COLUMN geocoding_confidence TEXT"
]

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
