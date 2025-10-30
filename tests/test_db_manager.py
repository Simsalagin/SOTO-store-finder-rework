"""Tests for database manager."""

import pytest
from datetime import datetime
from database.db_manager import DatabaseManager


@pytest.fixture
def temp_db(tmp_path):
    """Create temporary database for testing."""
    db_path = tmp_path / "test.db"
    db = DatabaseManager(str(db_path))
    yield db
    db.close()


def test_database_creation(temp_db):
    """Test database and tables are created."""
    assert temp_db.conn is not None
    assert temp_db.get_store_count() == 0


def test_insert_store(temp_db):
    """Test inserting a new store."""
    store_data = {
        'market_id': 12345,
        'name': 'Test Store',
        'street': 'Test Street 1',
        'zip': '12345',
        'city': 'Test City',
        'latitude': 52.5200,
        'longitude': 13.4050,
        'phone': '+49 123 456789',
        'email': 'test@example.com',
        'country_code': 'DE',
        'status': 'active',
        'opening_day': '2020-01-01',
        'is_loyalty_market': True,
        'google_maps_link': 'https://maps.google.com/test'
    }

    market_id = temp_db.upsert_store(store_data)
    assert market_id == 12345
    assert temp_db.get_store_count() == 1


def test_update_store(temp_db):
    """Test updating an existing store."""
    store_data = {
        'market_id': 12345,
        'name': 'Test Store',
        'city': 'Test City'
    }

    # Insert
    temp_db.upsert_store(store_data)
    assert temp_db.get_store_count() == 1

    # Update
    store_data['name'] = 'Updated Store'
    temp_db.upsert_store(store_data)

    # Should still have only 1 store
    assert temp_db.get_store_count() == 1

    # Verify update
    stores = temp_db.get_all_stores()
    assert stores[0]['name'] == 'Updated Store'


def test_duplicate_prevention(temp_db):
    """Test that duplicate market_ids are prevented."""
    store_data = {
        'market_id': 12345,
        'name': 'Test Store',
        'city': 'Test City'
    }

    # Insert same store twice
    temp_db.upsert_store(store_data)
    temp_db.upsert_store(store_data)

    # Should only have 1 store
    assert temp_db.get_store_count() == 1


def test_insert_opening_hours(temp_db):
    """Test inserting opening hours."""
    store_data = {
        'market_id': 12345,
        'name': 'Test Store',
        'city': 'Test City'
    }
    temp_db.upsert_store(store_data)

    hours = [
        {
            'weekday': 'Monday',
            'open_from': '09:00',
            'open_until': '18:00'
        },
        {
            'weekday': 'Tuesday',
            'open_from': '09:00',
            'open_until': '18:00'
        }
    ]

    temp_db.insert_opening_hours(12345, hours)

    # Verify hours were inserted
    cursor = temp_db.conn.cursor()
    count = cursor.execute(
        "SELECT COUNT(*) FROM opening_hours WHERE market_id = ?",
        (12345,)
    ).fetchone()[0]
    assert count == 2


def test_get_all_stores(temp_db):
    """Test retrieving all stores."""
    stores_data = [
        {'market_id': 1, 'name': 'Store 1', 'city': 'City 1'},
        {'market_id': 2, 'name': 'Store 2', 'city': 'City 2'},
        {'market_id': 3, 'name': 'Store 3', 'city': 'City 3'}
    ]

    for store in stores_data:
        temp_db.upsert_store(store)

    all_stores = temp_db.get_all_stores()
    assert len(all_stores) == 3
    assert all_stores[0]['name'] == 'Store 1'


def test_context_manager(tmp_path):
    """Test database context manager."""
    db_path = tmp_path / "test.db"

    with DatabaseManager(str(db_path)) as db:
        store_data = {'market_id': 12345, 'name': 'Test', 'city': 'Test'}
        db.upsert_store(store_data)

    # Database should be closed after context
    # Verify data persists
    with DatabaseManager(str(db_path)) as db:
        assert db.get_store_count() == 1
