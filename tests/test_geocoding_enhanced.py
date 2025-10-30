"""
Tests for enhanced geocoding service with 3-tier strategy.

This module tests the enhanced geocoding logic that uses:
- Tier 1: Overpass POI search with validation
- Tier 2: Scraper coordinates
- Tier 3: Nominatim address-level fallback

It also tests address correction when POI and scraper coordinates conflict.
"""

import pytest
import responses
from unittest.mock import Mock, patch
import sqlite3


@pytest.fixture
def test_db_path(tmp_path):
    """Provide a temporary database path."""
    return str(tmp_path / "test_stores.db")


@pytest.fixture
def setup_test_store(test_db_path):
    """Setup test database with a sample store."""
    from database.db_manager import DatabaseManager

    db = DatabaseManager(test_db_path)

    store_data = {
        'market_id': 1,
        'name': 'Denns BioMarkt Bamberg',
        'street': 'Obere Königstr. 20',
        'zip': '96052',
        'city': 'Bamberg',
        'latitude': 49.89687,
        'longitude': 10.89395
    }

    db.upsert_store(store_data)
    db.close()

    return test_db_path


@responses.activate
def test_geocode_store_tier1_very_high(setup_test_store):
    """Test Tier 1: POI found and validated by scraper (< 100m) → very_high confidence."""
    from services.geocoding_service import GeocodingService

    # Mock Overpass POI response (within 100m of scraper coords)
    mock_poi_response = {
        "version": 0.6,
        "elements": [{
            "type": "node",
            "id": 123456,
            "lat": 49.8968299,  # ~25m from scraper
            "lon": 10.8938646,
            "tags": {
                "name": "Denns BioMarkt",
                "shop": "supermarket",
                "addr:street": "Obere Königstraße",
                "addr:housenumber": "20",
                "addr:postcode": "96052",
                "addr:city": "Bamberg"
            }
        }]
    }

    responses.add(
        responses.POST,
        "https://overpass-api.de/api/interpreter",
        json=mock_poi_response,
        status=200
    )

    service = GeocodingService(setup_test_store)

    # Get store
    cursor = service.db.conn.cursor()
    cursor.execute("SELECT * FROM stores WHERE market_id = 1")
    columns = [desc[0] for desc in cursor.description]
    row = cursor.fetchone()
    store = dict(zip(columns, row))

    # Geocode
    result = service.geocode_store_enhanced(store)

    assert result is not None
    assert result['tier'] == 1
    assert result['geocoding_source'] == 'overpass_poi'
    assert result['geocoding_confidence'] == 'very_high'
    assert result['final_latitude'] == 49.8968299
    assert result['final_longitude'] == 10.8938646
    assert result.get('update_address') == False  # No address update needed


@responses.activate
def test_geocode_store_tier1_high(setup_test_store):
    """Test Tier 1: POI found but no scraper to validate → high confidence."""
    from services.geocoding_service import GeocodingService
    from database.db_manager import DatabaseManager

    # Create store without valid coordinates
    db = DatabaseManager(setup_test_store)
    store_data = {
        'market_id': 2,
        'name': 'Denns BioMarkt München',
        'street': 'Teststraße 1',
        'zip': '80331',
        'city': 'München',
        'latitude': 0,  # Invalid
        'longitude': 0   # Invalid
    }
    db.upsert_store(store_data)
    db.close()

    # Mock Overpass POI response
    mock_poi_response = {
        "version": 0.6,
        "elements": [{
            "type": "node",
            "id": 789,
            "lat": 48.1351,
            "lon": 11.5820,
            "tags": {"name": "Denns BioMarkt"}
        }]
    }

    responses.add(
        responses.POST,
        "https://overpass-api.de/api/interpreter",
        json=mock_poi_response,
        status=200
    )

    service = GeocodingService(setup_test_store)

    cursor = service.db.conn.cursor()
    cursor.execute("SELECT * FROM stores WHERE market_id = 2")
    columns = [desc[0] for desc in cursor.description]
    row = cursor.fetchone()
    store = dict(zip(columns, row))

    result = service.geocode_store_enhanced(store)

    assert result is not None
    assert result['tier'] == 1
    assert result['geocoding_confidence'] == 'high'
    assert result['geocoding_source'] == 'overpass_poi'


@responses.activate
@patch('services.geocoding_service.logger')
def test_geocode_store_tier1_conflict_updates_address(mock_logger, setup_test_store):
    """Test Tier 1: POI found but > 100m from scraper → high confidence + address update + warning."""
    from services.geocoding_service import GeocodingService
    from database.db_manager import DatabaseManager

    # Create store with WRONG address (Turnstraße example)
    db = DatabaseManager(setup_test_store)
    store_data = {
        'market_id': 3,
        'name': 'Denns BioMarkt Erlangen',
        'street': 'Turnstraße 9',  # Wrong address
        'zip': '91054',
        'city': 'Erlangen',
        'latitude': 49.6004,  # Wrong location
        'longitude': 11.0071
    }
    db.upsert_store(store_data)
    db.close()

    # Mock Overpass POI response with CORRECT address (Paul-Gossen-Str.)
    mock_poi_response = {
        "version": 0.6,
        "elements": [{
            "type": "node",
            "id": 999,
            "lat": 49.57889,  # Correct location (~2.4km away)
            "lon": 11.00271,
            "tags": {
                "name": "Denns BioMarkt",
                "addr:street": "Paul-Gossen-Straße",
                "addr:housenumber": "69",
                "addr:postcode": "91052",
                "addr:city": "Erlangen"
            }
        }]
    }

    responses.add(
        responses.POST,
        "https://overpass-api.de/api/interpreter",
        json=mock_poi_response,
        status=200
    )

    service = GeocodingService(setup_test_store)

    cursor = service.db.conn.cursor()
    cursor.execute("SELECT * FROM stores WHERE market_id = 3")
    columns = [desc[0] for desc in cursor.description]
    row = cursor.fetchone()
    store = dict(zip(columns, row))

    result = service.geocode_store_enhanced(store)

    assert result is not None
    assert result['tier'] == 1
    assert result['geocoding_confidence'] == 'high'  # Not very_high due to conflict
    assert result['geocoding_source'] == 'overpass_poi'
    assert result['update_address'] == True  # 🔥 Address should be updated
    assert result['new_street'] == '69 Paul-Gossen-Straße'
    assert result['new_zip'] == '91052'
    assert result['new_city'] == 'Erlangen'

    # Verify warning was logged
    mock_logger.warning.assert_called_once()
    warning_call = mock_logger.warning.call_args[0][0]
    assert 'mismatch' in warning_call.lower()


@responses.activate
def test_geocode_store_tier2_medium(setup_test_store):
    """Test Tier 2: No POI found, use scraper coordinates → medium confidence."""
    from services.geocoding_service import GeocodingService

    # Mock empty Overpass responses (all variants fail)
    for _ in range(5):  # 3 exact + 2 fuzzy
        responses.add(
            responses.POST,
            "https://overpass-api.de/api/interpreter",
            json={"version": 0.6, "elements": []},
            status=200
        )

    service = GeocodingService(setup_test_store)

    cursor = service.db.conn.cursor()
    cursor.execute("SELECT * FROM stores WHERE market_id = 1")
    columns = [desc[0] for desc in cursor.description]
    row = cursor.fetchone()
    store = dict(zip(columns, row))

    result = service.geocode_store_enhanced(store)

    assert result is not None
    assert result['tier'] == 2
    assert result['geocoding_confidence'] == 'medium'
    assert result['geocoding_source'] == 'scraper'
    assert result['final_latitude'] == 49.89687
    assert result['final_longitude'] == 10.89395
    assert result.get('update_address') == False


@responses.activate
def test_geocode_store_tier3_low(setup_test_store):
    """Test Tier 3: No POI, no scraper → Nominatim fallback → low confidence."""
    from services.geocoding_service import GeocodingService
    from database.db_manager import DatabaseManager

    # Create store with invalid scraper coords
    db = DatabaseManager(setup_test_store)
    store_data = {
        'market_id': 4,
        'name': 'Denns BioMarkt Test',
        'street': 'Teststraße 1',
        'zip': '12345',
        'city': 'Teststadt',
        'country_code': 'DE',
        'latitude': 0,
        'longitude': 0
    }
    db.upsert_store(store_data)
    db.close()

    # Mock empty Overpass responses
    for _ in range(5):
        responses.add(
            responses.POST,
            "https://overpass-api.de/api/interpreter",
            json={"version": 0.6, "elements": []},
            status=200
        )

    # Mock Nominatim response
    mock_nominatim_response = [{
        "lat": "52.5200",
        "lon": "13.4050",
        "display_name": "Teststraße 1, Teststadt, Germany"
    }]

    responses.add(
        responses.GET,
        "https://nominatim.openstreetmap.org/search",
        json=mock_nominatim_response,
        status=200
    )

    service = GeocodingService(setup_test_store)

    cursor = service.db.conn.cursor()
    cursor.execute("SELECT * FROM stores WHERE market_id = 4")
    columns = [desc[0] for desc in cursor.description]
    row = cursor.fetchone()
    store = dict(zip(columns, row))

    result = service.geocode_store_enhanced(store)

    assert result is not None
    assert result['tier'] == 3
    assert result['geocoding_confidence'] == 'low'
    assert result['geocoding_source'] == 'nominatim_address'
    assert result['final_latitude'] == 52.5200
    assert result['final_longitude'] == 13.4050


@responses.activate
def test_final_coords_updated_correctly(setup_test_store):
    """Test that final_latitude and final_longitude are updated in database."""
    from services.geocoding_service import GeocodingService

    # Mock Overpass response
    responses.add(
        responses.POST,
        "https://overpass-api.de/api/interpreter",
        json={
            "version": 0.6,
            "elements": [{
                "type": "node",
                "id": 123,
                "lat": 49.8968,
                "lon": 10.8939,
                "tags": {"name": "Denns BioMarkt"}
            }]
        },
        status=200
    )

    service = GeocodingService(setup_test_store)

    cursor = service.db.conn.cursor()
    cursor.execute("SELECT * FROM stores WHERE market_id = 1")
    columns = [desc[0] for desc in cursor.description]
    row = cursor.fetchone()
    store = dict(zip(columns, row))

    result = service.geocode_store_enhanced(store)
    service.update_store_with_final_coords(1, result)

    # Verify database update
    cursor.execute("SELECT final_latitude, final_longitude FROM stores WHERE market_id = 1")
    row = cursor.fetchone()

    assert row[0] == 49.8968
    assert row[1] == 10.8939


@responses.activate
def test_geocoding_metadata_updated(setup_test_store):
    """Test that geocoding_source and geocoding_confidence are updated."""
    from services.geocoding_service import GeocodingService

    # Mock Overpass response
    responses.add(
        responses.POST,
        "https://overpass-api.de/api/interpreter",
        json={
            "version": 0.6,
            "elements": [{
                "type": "node",
                "id": 123,
                "lat": 49.8968,
                "lon": 10.8939,
                "tags": {"name": "Denns BioMarkt"}
            }]
        },
        status=200
    )

    service = GeocodingService(setup_test_store)

    cursor = service.db.conn.cursor()
    cursor.execute("SELECT * FROM stores WHERE market_id = 1")
    columns = [desc[0] for desc in cursor.description]
    row = cursor.fetchone()
    store = dict(zip(columns, row))

    result = service.geocode_store_enhanced(store)
    service.update_store_with_final_coords(1, result)

    # Verify metadata
    cursor.execute("""
        SELECT geocoding_source, geocoding_confidence, osm_checked
        FROM stores WHERE market_id = 1
    """)
    row = cursor.fetchone()

    assert row[0] == 'overpass_poi'
    assert row[1] == 'very_high'
    assert row[2] == 1  # osm_checked should be True


@responses.activate
def test_address_updated_from_osm_on_conflict(setup_test_store):
    """Test that address is updated from OSM when conflict detected."""
    from services.geocoding_service import GeocodingService
    from database.db_manager import DatabaseManager

    # Create store with wrong address
    db = DatabaseManager(setup_test_store)
    store_data = {
        'market_id': 5,
        'name': 'Denns BioMarkt Test',
        'street': 'Wrong Street 99',
        'zip': '00000',
        'city': 'Wrong City',
        'latitude': 50.0,
        'longitude': 10.0
    }
    db.upsert_store(store_data)
    db.close()

    # Mock Overpass with correct address far away (> 100m)
    responses.add(
        responses.POST,
        "https://overpass-api.de/api/interpreter",
        json={
            "version": 0.6,
            "elements": [{
                "type": "node",
                "id": 456,
                "lat": 50.1,  # > 100m away
                "lon": 10.1,
                "tags": {
                    "name": "Denns BioMarkt",
                    "addr:street": "Correct Street",
                    "addr:housenumber": "42",
                    "addr:postcode": "11111",
                    "addr:city": "Correct City"
                }
            }]
        },
        status=200
    )

    service = GeocodingService(setup_test_store)

    cursor = service.db.conn.cursor()
    cursor.execute("SELECT * FROM stores WHERE market_id = 5")
    columns = [desc[0] for desc in cursor.description]
    row = cursor.fetchone()
    store = dict(zip(columns, row))

    result = service.geocode_store_enhanced(store)
    service.update_store_with_final_coords(5, result)

    # Verify address was updated
    cursor.execute("SELECT street, zip, city FROM stores WHERE market_id = 5")
    row = cursor.fetchone()

    assert row[0] == '42 Correct Street'
    assert row[1] == '11111'
    assert row[2] == 'Correct City'


@responses.activate
def test_address_preserved_when_validated(setup_test_store):
    """Test that address is NOT changed when POI validates scraper (< 100m)."""
    from services.geocoding_service import GeocodingService

    # Mock Overpass with POI close to scraper (< 100m)
    responses.add(
        responses.POST,
        "https://overpass-api.de/api/interpreter",
        json={
            "version": 0.6,
            "elements": [{
                "type": "node",
                "id": 123,
                "lat": 49.89702,  # ~25m from scraper
                "lon": 10.89408,
                "tags": {
                    "name": "Denns BioMarkt",
                    "addr:street": "Different Street Name",  # Different from DB
                    "addr:housenumber": "99",
                    "addr:postcode": "99999"
                }
            }]
        },
        status=200
    )

    service = GeocodingService(setup_test_store)

    cursor = service.db.conn.cursor()
    cursor.execute("SELECT * FROM stores WHERE market_id = 1")
    columns = [desc[0] for desc in cursor.description]
    row = cursor.fetchone()
    store = dict(zip(columns, row))

    original_street = store['street']
    original_zip = store['zip']

    result = service.geocode_store_enhanced(store)
    service.update_store_with_final_coords(1, result)

    # Verify address was NOT changed
    cursor.execute("SELECT street, zip FROM stores WHERE market_id = 1")
    row = cursor.fetchone()

    assert row[0] == original_street  # Should preserve original
    assert row[1] == original_zip
