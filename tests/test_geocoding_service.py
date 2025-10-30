"""Tests for geocoding service."""

import pytest
from unittest.mock import Mock, patch


@pytest.fixture
def test_db_path(tmp_path):
    """Provide a temporary database path."""
    return str(tmp_path / "test_stores.db")


@pytest.fixture
def sample_stores():
    """Provide sample store data."""
    return [
        {
            'market_id': 1,
            'name': 'Store 1',
            'street': 'Test Street 1',
            'city': 'Berlin',
            'zip': '10115',
            'country_code': 'DE'
        },
        {
            'market_id': 2,
            'name': 'Store 2',
            'street': 'Test Street 2',
            'city': 'München',
            'zip': '80331',
            'country_code': 'DE'
        },
        {
            'market_id': 3,
            'name': 'Store 3',
            'street': 'Test Street 3',
            'city': 'Hamburg',
            'zip': '20095',
            'country_code': 'DE'
        }
    ]


def test_geocoding_service_import():
    """Test that we can import the GeocodingService."""
    from services.geocoding_service import GeocodingService

    service = GeocodingService()
    assert service is not None


def test_get_stores_needing_geocoding(test_db_path, sample_stores):
    """Test getting stores that need OSM geocoding."""
    from database.db_manager import DatabaseManager
    from services.geocoding_service import GeocodingService

    # Setup database with stores
    db = DatabaseManager(test_db_path)
    for store in sample_stores:
        db.upsert_store(store)

    # Get stores needing geocoding
    service = GeocodingService(db_path=test_db_path)
    stores = service.get_stores_needing_geocoding()

    # All stores should need geocoding (osm_checked = 0 by default)
    assert len(stores) == 3
    assert all(store['osm_checked'] == 0 for store in stores)

    db.close()
    service.close()


def test_get_stores_needing_geocoding_with_limit(test_db_path, sample_stores):
    """Test getting stores with limit parameter."""
    from database.db_manager import DatabaseManager
    from services.geocoding_service import GeocodingService

    # Setup database with stores
    db = DatabaseManager(test_db_path)
    for store in sample_stores:
        db.upsert_store(store)

    # Get stores with limit
    service = GeocodingService(db_path=test_db_path)
    stores = service.get_stores_needing_geocoding(limit=2)

    # Should only get 2 stores
    assert len(stores) == 2

    db.close()
    service.close()


def test_get_stores_excludes_already_checked(test_db_path, sample_stores):
    """Test that already-checked stores are excluded."""
    from database.db_manager import DatabaseManager
    from services.geocoding_service import GeocodingService

    # Setup database with stores
    db = DatabaseManager(test_db_path)
    for store in sample_stores:
        db.upsert_store(store)

    # Mark first store as checked
    cursor = db.conn.cursor()
    cursor.execute("UPDATE stores SET osm_checked = 1 WHERE market_id = 1")
    db.conn.commit()

    # Get stores needing geocoding
    service = GeocodingService(db_path=test_db_path)
    stores = service.get_stores_needing_geocoding()

    # Should only get 2 unchecked stores
    assert len(stores) == 2
    assert all(store['market_id'] != 1 for store in stores)

    db.close()
    service.close()


@patch('services.geocoding_service.NominatimClient')
def test_geocode_single_store_success(mock_nominatim, test_db_path, sample_stores):
    """Test successfully geocoding a single store."""
    from database.db_manager import DatabaseManager
    from services.geocoding_service import GeocodingService

    # Setup database
    db = DatabaseManager(test_db_path)
    db.upsert_store(sample_stores[0])

    # Mock Nominatim response
    mock_client = Mock()
    mock_client.geocode.return_value = {
        'latitude': 52.5200,
        'longitude': 13.4050,
        'display_name': 'Berlin, Deutschland'
    }
    mock_nominatim.return_value = mock_client

    # Geocode the store
    service = GeocodingService(db_path=test_db_path)
    result = service.geocode_store(sample_stores[0])

    # Verify geocoding was called with correct parameters
    mock_client.geocode.assert_called_once()
    call_kwargs = mock_client.geocode.call_args[1]
    assert call_kwargs['city'] == 'Berlin'
    assert call_kwargs['street'] == 'Test Street 1'
    assert call_kwargs['postal_code'] == '10115'

    # Verify result
    assert result is True

    # Verify database was updated
    cursor = db.conn.cursor()
    cursor.execute("""
        SELECT osm_latitude, osm_longitude, osm_display_name, osm_checked
        FROM stores WHERE market_id = 1
    """)
    row = cursor.fetchone()

    assert row[0] == 52.5200  # osm_latitude
    assert row[1] == 13.4050  # osm_longitude
    assert row[2] == 'Berlin, Deutschland'  # osm_display_name
    assert row[3] == 1  # osm_checked

    db.close()
    service.close()


@patch('services.geocoding_service.NominatimClient')
def test_geocode_single_store_not_found(mock_nominatim, test_db_path, sample_stores):
    """Test geocoding a store when no results are found."""
    from database.db_manager import DatabaseManager
    from services.geocoding_service import GeocodingService

    # Setup database
    db = DatabaseManager(test_db_path)
    db.upsert_store(sample_stores[0])

    # Mock Nominatim response (no results)
    mock_client = Mock()
    mock_client.geocode.return_value = None
    mock_nominatim.return_value = mock_client

    # Geocode the store
    service = GeocodingService(db_path=test_db_path)
    result = service.geocode_store(sample_stores[0])

    # Verify result is False (not found)
    assert result is False

    # Verify store is marked as checked even though geocoding failed
    cursor = db.conn.cursor()
    cursor.execute("SELECT osm_checked, osm_latitude FROM stores WHERE market_id = 1")
    row = cursor.fetchone()

    assert row[0] == 1  # osm_checked should be 1
    assert row[1] is None  # osm_latitude should still be None

    db.close()
    service.close()


@patch('services.geocoding_service.NominatimClient')
def test_geocode_store_handles_missing_country_code(mock_nominatim, test_db_path):
    """Test geocoding when country_code is missing."""
    from database.db_manager import DatabaseManager
    from services.geocoding_service import GeocodingService

    # Store without country_code
    store = {
        'market_id': 99,
        'name': 'Store Without Country',
        'street': 'Test Street',
        'city': 'Berlin',
        'zip': '10115'
    }

    # Setup database
    db = DatabaseManager(test_db_path)
    db.upsert_store(store)

    # Mock Nominatim response
    mock_client = Mock()
    mock_client.geocode.return_value = {
        'latitude': 52.5200,
        'longitude': 13.4050,
        'display_name': 'Berlin, Deutschland'
    }
    mock_nominatim.return_value = mock_client

    # Geocode the store
    service = GeocodingService(db_path=test_db_path)
    result = service.geocode_store(store)

    # Verify geocoding was called (should handle None country)
    mock_client.geocode.assert_called_once()
    assert result is True

    db.close()
    service.close()


def test_update_store_with_osm_data(test_db_path, sample_stores):
    """Test updating a store with OSM data."""
    from database.db_manager import DatabaseManager
    from services.geocoding_service import GeocodingService

    # Setup database
    db = DatabaseManager(test_db_path)
    db.upsert_store(sample_stores[0])

    # Update with OSM data
    service = GeocodingService(db_path=test_db_path)
    osm_data = {
        'latitude': 52.5200,
        'longitude': 13.4050,
        'display_name': 'Berlin, Deutschland'
    }

    service.update_store_with_osm_data(1, osm_data)

    # Verify update
    cursor = db.conn.cursor()
    cursor.execute("""
        SELECT osm_latitude, osm_longitude, osm_display_name, osm_checked
        FROM stores WHERE market_id = 1
    """)
    row = cursor.fetchone()

    assert row[0] == 52.5200
    assert row[1] == 13.4050
    assert row[2] == 'Berlin, Deutschland'
    assert row[3] == 1

    db.close()
    service.close()


def test_mark_store_as_checked(test_db_path, sample_stores):
    """Test marking a store as checked without OSM data."""
    from database.db_manager import DatabaseManager
    from services.geocoding_service import GeocodingService

    # Setup database
    db = DatabaseManager(test_db_path)
    db.upsert_store(sample_stores[0])

    # Mark as checked
    service = GeocodingService(db_path=test_db_path)
    service.mark_store_as_checked(1)

    # Verify
    cursor = db.conn.cursor()
    cursor.execute("SELECT osm_checked, osm_checked_at FROM stores WHERE market_id = 1")
    row = cursor.fetchone()

    assert row[0] == 1  # osm_checked
    assert row[1] is not None  # osm_checked_at

    db.close()
    service.close()


@patch('services.geocoding_service.NominatimClient')
def test_geocode_all_stores(mock_nominatim, test_db_path, sample_stores):
    """Test batch geocoding all stores."""
    from database.db_manager import DatabaseManager
    from services.geocoding_service import GeocodingService

    # Setup database with multiple stores
    db = DatabaseManager(test_db_path)
    for store in sample_stores:
        db.upsert_store(store)

    # Mock Nominatim responses
    mock_client = Mock()
    mock_client.geocode.side_effect = [
        {'latitude': 52.5200, 'longitude': 13.4050, 'display_name': 'Berlin, Deutschland'},
        {'latitude': 48.1351, 'longitude': 11.5820, 'display_name': 'München, Deutschland'},
        {'latitude': 53.5511, 'longitude': 9.9937, 'display_name': 'Hamburg, Deutschland'}
    ]
    mock_nominatim.return_value = mock_client

    # Geocode all stores
    service = GeocodingService(db_path=test_db_path)
    stats = service.geocode_all_stores()

    # Verify stats
    assert stats['total'] == 3
    assert stats['successful'] == 3
    assert stats['failed'] == 0

    # Verify all stores were geocoded
    cursor = db.conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM stores WHERE osm_checked = 1")
    count = cursor.fetchone()[0]
    assert count == 3

    db.close()
    service.close()


@patch('services.geocoding_service.NominatimClient')
def test_geocode_all_stores_with_limit(mock_nominatim, test_db_path, sample_stores):
    """Test batch geocoding with limit."""
    from database.db_manager import DatabaseManager
    from services.geocoding_service import GeocodingService

    # Setup database with multiple stores
    db = DatabaseManager(test_db_path)
    for store in sample_stores:
        db.upsert_store(store)

    # Mock Nominatim responses
    mock_client = Mock()
    mock_client.geocode.side_effect = [
        {'latitude': 52.5200, 'longitude': 13.4050, 'display_name': 'Berlin, Deutschland'},
        {'latitude': 48.1351, 'longitude': 11.5820, 'display_name': 'München, Deutschland'}
    ]
    mock_nominatim.return_value = mock_client

    # Geocode with limit of 2
    service = GeocodingService(db_path=test_db_path)
    stats = service.geocode_all_stores(limit=2)

    # Verify stats
    assert stats['total'] == 2
    assert stats['successful'] == 2
    assert stats['failed'] == 0

    # Verify only 2 stores were geocoded
    cursor = db.conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM stores WHERE osm_checked = 1")
    count = cursor.fetchone()[0]
    assert count == 2

    db.close()
    service.close()


@patch('services.geocoding_service.NominatimClient')
def test_geocode_all_stores_with_failures(mock_nominatim, test_db_path, sample_stores):
    """Test batch geocoding with some failures."""
    from database.db_manager import DatabaseManager
    from services.geocoding_service import GeocodingService

    # Setup database with multiple stores
    db = DatabaseManager(test_db_path)
    for store in sample_stores:
        db.upsert_store(store)

    # Mock Nominatim responses (one success, two failures)
    mock_client = Mock()
    mock_client.geocode.side_effect = [
        {'latitude': 52.5200, 'longitude': 13.4050, 'display_name': 'Berlin, Deutschland'},
        None,  # Not found
        None   # Not found
    ]
    mock_nominatim.return_value = mock_client

    # Geocode all stores
    service = GeocodingService(db_path=test_db_path)
    stats = service.geocode_all_stores()

    # Verify stats
    assert stats['total'] == 3
    assert stats['successful'] == 1
    assert stats['failed'] == 2

    # Verify all stores were marked as checked
    cursor = db.conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM stores WHERE osm_checked = 1")
    count = cursor.fetchone()[0]
    assert count == 3

    db.close()
    service.close()
