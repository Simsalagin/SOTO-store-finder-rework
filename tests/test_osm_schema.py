"""Tests for OSM schema migration."""

import sqlite3
import os
import pytest


@pytest.fixture
def test_db_path(tmp_path):
    """Provide a temporary database path."""
    return str(tmp_path / "test_stores.db")


def test_stores_table_has_osm_columns(test_db_path):
    """Test that stores table includes OSM-related columns."""
    # Import here to trigger table creation
    from database.db_manager import DatabaseManager

    # Create database with current schema
    db = DatabaseManager(test_db_path)

    # Query the schema to check for OSM columns
    cursor = db.conn.cursor()
    cursor.execute("PRAGMA table_info(stores)")
    columns = cursor.fetchall()
    column_names = [col[1] for col in columns]

    # Assert OSM columns exist
    assert 'osm_latitude' in column_names, "Missing osm_latitude column"
    assert 'osm_longitude' in column_names, "Missing osm_longitude column"
    assert 'osm_checked' in column_names, "Missing osm_checked column"
    assert 'osm_checked_at' in column_names, "Missing osm_checked_at column"
    assert 'osm_display_name' in column_names, "Missing osm_display_name column"

    db.close()


def test_osm_checked_defaults_to_false(test_db_path):
    """Test that osm_checked defaults to 0 (false) for new stores."""
    from database.db_manager import DatabaseManager

    db = DatabaseManager(test_db_path)

    # Insert a test store
    store_data = {
        'market_id': 1,
        'name': 'Test Store',
        'street': 'Test Street 1',
        'city': 'Test City',
        'zip': '12345'
    }

    db.upsert_store(store_data)

    # Query the store
    cursor = db.conn.cursor()
    cursor.execute("SELECT osm_checked FROM stores WHERE market_id = 1")
    result = cursor.fetchone()

    # Assert osm_checked is 0 (false) by default
    assert result[0] == 0, "osm_checked should default to 0"

    db.close()


def test_can_update_osm_data(test_db_path):
    """Test that we can update OSM data for a store."""
    from database.db_manager import DatabaseManager

    db = DatabaseManager(test_db_path)

    # Insert a test store
    store_data = {
        'market_id': 1,
        'name': 'Test Store',
        'street': 'Test Street 1',
        'city': 'Test City',
        'zip': '12345'
    }

    db.upsert_store(store_data)

    # Update with OSM data
    cursor = db.conn.cursor()
    cursor.execute("""
        UPDATE stores
        SET osm_latitude = ?, osm_longitude = ?, osm_display_name = ?,
            osm_checked = ?, osm_checked_at = CURRENT_TIMESTAMP
        WHERE market_id = ?
    """, (52.5200, 13.4050, 'Berlin, Germany', 1, 1))
    db.conn.commit()

    # Query the updated store
    cursor.execute("""
        SELECT osm_latitude, osm_longitude, osm_display_name, osm_checked
        FROM stores WHERE market_id = 1
    """)
    result = cursor.fetchone()

    # Assert OSM data was updated
    assert result[0] == 52.5200, "osm_latitude should be updated"
    assert result[1] == 13.4050, "osm_longitude should be updated"
    assert result[2] == 'Berlin, Germany', "osm_display_name should be updated"
    assert result[3] == 1, "osm_checked should be 1"

    db.close()
