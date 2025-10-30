"""Tests for final coordinates schema migration."""

import sqlite3
import pytest


@pytest.fixture
def test_db_path(tmp_path):
    """Provide a temporary database path."""
    return str(tmp_path / "test_stores.db")


def test_stores_table_has_final_coord_columns(test_db_path):
    """Test that stores table includes final coordinate columns for Leaflet."""
    from database.db_manager import DatabaseManager

    # Create database with current schema
    db = DatabaseManager(test_db_path)

    # Query the schema to check for final coordinate columns
    cursor = db.conn.cursor()
    cursor.execute("PRAGMA table_info(stores)")
    columns = cursor.fetchall()
    column_names = [col[1] for col in columns]

    # Assert final coordinate columns exist
    assert 'final_latitude' in column_names, "Missing final_latitude column"
    assert 'final_longitude' in column_names, "Missing final_longitude column"
    assert 'geocoding_source' in column_names, "Missing geocoding_source column"
    assert 'geocoding_confidence' in column_names, "Missing geocoding_confidence column"

    db.close()


def test_final_coords_default_to_null(test_db_path):
    """Test that final coordinates default to NULL for new stores."""
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
    cursor.execute("""
        SELECT final_latitude, final_longitude, geocoding_source, geocoding_confidence
        FROM stores WHERE market_id = 1
    """)
    result = cursor.fetchone()

    # Assert all new columns default to NULL
    assert result[0] is None, "final_latitude should default to NULL"
    assert result[1] is None, "final_longitude should default to NULL"
    assert result[2] is None, "geocoding_source should default to NULL"
    assert result[3] is None, "geocoding_confidence should default to NULL"

    db.close()


def test_can_update_final_coords(test_db_path):
    """Test that we can update final coordinates and metadata."""
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

    # Update with final coordinates
    cursor = db.conn.cursor()
    cursor.execute("""
        UPDATE stores
        SET final_latitude = ?, final_longitude = ?,
            geocoding_source = ?, geocoding_confidence = ?
        WHERE market_id = ?
    """, (52.5200, 13.4050, 'overpass_poi', 'high', 1))
    db.conn.commit()

    # Query the updated store
    cursor.execute("""
        SELECT final_latitude, final_longitude, geocoding_source, geocoding_confidence
        FROM stores WHERE market_id = 1
    """)
    result = cursor.fetchone()

    # Assert final coordinates were updated
    assert result[0] == 52.5200, "final_latitude should be updated"
    assert result[1] == 13.4050, "final_longitude should be updated"
    assert result[2] == 'overpass_poi', "geocoding_source should be updated"
    assert result[3] == 'high', "geocoding_confidence should be updated"

    db.close()


def test_geocoding_source_values(test_db_path):
    """Test that geocoding_source accepts expected values."""
    from database.db_manager import DatabaseManager

    db = DatabaseManager(test_db_path)

    # Test all three source values
    sources = ['overpass_poi', 'scraper', 'nominatim_address']

    for idx, source in enumerate(sources, start=1):
        store_data = {
            'market_id': idx,
            'name': f'Test Store {idx}',
            'city': 'Test City',
        }
        db.upsert_store(store_data)

        cursor = db.conn.cursor()
        cursor.execute("""
            UPDATE stores SET geocoding_source = ? WHERE market_id = ?
        """, (source, idx))
        db.conn.commit()

    # Verify all sources were stored
    cursor = db.conn.cursor()
    cursor.execute("SELECT geocoding_source FROM stores ORDER BY market_id")
    results = [row[0] for row in cursor.fetchall()]

    assert results == sources, "All geocoding sources should be stored correctly"

    db.close()


def test_geocoding_confidence_values(test_db_path):
    """Test that geocoding_confidence accepts expected values."""
    from database.db_manager import DatabaseManager

    db = DatabaseManager(test_db_path)

    # Test all three confidence values
    confidences = ['high', 'medium', 'low']

    for idx, confidence in enumerate(confidences, start=1):
        store_data = {
            'market_id': idx,
            'name': f'Test Store {idx}',
            'city': 'Test City',
        }
        db.upsert_store(store_data)

        cursor = db.conn.cursor()
        cursor.execute("""
            UPDATE stores SET geocoding_confidence = ? WHERE market_id = ?
        """, (confidence, idx))
        db.conn.commit()

    # Verify all confidence levels were stored
    cursor = db.conn.cursor()
    cursor.execute("SELECT geocoding_confidence FROM stores ORDER BY market_id")
    results = [row[0] for row in cursor.fetchall()]

    assert results == confidences, "All confidence levels should be stored correctly"

    db.close()
