"""
Tests for OverpassClient name variants and address extraction.

These tests cover the enhanced POI search functionality that tries multiple
name variants and extracts address data from OSM tags.
"""

import pytest
import responses
from src.services.overpass_client import OverpassClient


def test_extract_base_name_removes_city():
    """Test that city name is removed from store name."""
    client = OverpassClient()

    # Test with city at end
    assert client._extract_base_name("Denns BioMarkt Erlangen") == "Denns BioMarkt"
    assert client._extract_base_name("Denns BioMarkt München") == "Denns BioMarkt"
    assert client._extract_base_name("Speisekammer Hof") == "Speisekammer Hof"  # Only 2 words, keep all

    # Test edge cases
    assert client._extract_base_name("SingleWord") == "SingleWord"
    assert client._extract_base_name("Two Words") == "Two Words"
    assert client._extract_base_name("") == ""


@responses.activate
def test_search_poi_with_variants_exact_success():
    """Test that search_poi_with_variants tries multiple exact name variants."""
    client = OverpassClient()

    # Mock response for second variant "Denns Biomarkt" (lowercase i)
    mock_response = {
        "version": 0.6,
        "elements": [
            {
                "type": "node",
                "id": 123456,
                "lat": 49.8968,
                "lon": 10.8939,
                "tags": {
                    "name": "Denns Biomarkt",
                    "shop": "supermarket",
                    "addr:street": "Obere Königstraße",
                    "addr:housenumber": "20",
                    "addr:postcode": "96052",
                    "addr:city": "Bamberg"
                }
            }
        ]
    }

    # First call (exact "Denns BioMarkt") will return empty
    responses.add(
        responses.POST,
        "https://overpass-api.de/api/interpreter",
        json={"version": 0.6, "elements": []},
        status=200
    )

    # Second call (exact "Denns Biomarkt" with lowercase i) will succeed
    responses.add(
        responses.POST,
        "https://overpass-api.de/api/interpreter",
        json=mock_response,
        status=200
    )

    result = client.search_poi_with_variants("Denns BioMarkt Bamberg", 49.8968, 10.8939, radius=100)

    assert result is not None
    assert result['latitude'] == 49.8968
    assert result['longitude'] == 10.8939
    assert result['name'] == "Denns Biomarkt"
    assert result['matched_variant'] == "Denns Biomarkt"


@responses.activate
def test_search_poi_with_variants_fuzzy_success():
    """Test that search falls back to fuzzy matching when exact fails."""
    client = OverpassClient()

    # Mock empty responses for all 3 exact variants
    for _ in range(3):
        responses.add(
            responses.POST,
            "https://overpass-api.de/api/interpreter",
            json={"version": 0.6, "elements": []},
            status=200
        )

    # Mock successful fuzzy match with "denn's"
    mock_response = {
        "version": 0.6,
        "elements": [
            {
                "type": "node",
                "id": 789,
                "lat": 50.3054,
                "lon": 11.9185,
                "tags": {
                    "name": "denn's Biomarkt",
                    "shop": "supermarket"
                }
            }
        ]
    }

    responses.add(
        responses.POST,
        "https://overpass-api.de/api/interpreter",
        json=mock_response,
        status=200
    )

    result = client.search_poi_with_variants("Denns BioMarkt Hof", 50.3054, 11.9185, radius=100)

    assert result is not None
    assert result['name'] == "denn's Biomarkt"
    assert result['matched_variant'] == "fuzzy:denn's"


@responses.activate
def test_search_poi_with_variants_returns_first_match():
    """Test that search stops at first successful match."""
    client = OverpassClient()

    # Mock successful response for first variant
    mock_response = {
        "version": 0.6,
        "elements": [
            {
                "type": "node",
                "id": 123,
                "lat": 50.0,
                "lon": 10.0,
                "tags": {"name": "Denns BioMarkt"}
            }
        ]
    }

    responses.add(
        responses.POST,
        "https://overpass-api.de/api/interpreter",
        json=mock_response,
        status=200
    )

    result = client.search_poi_with_variants("Denns BioMarkt City", 50.0, 10.0)

    # Should only make 1 API call and stop
    assert len(responses.calls) == 1
    assert result is not None
    assert result['matched_variant'] == "Denns BioMarkt"


@responses.activate
def test_parse_response_includes_address_tags():
    """Test that _parse_response extracts address tags from OSM."""
    client = OverpassClient()

    response_json = {
        "version": 0.6,
        "elements": [
            {
                "type": "node",
                "id": 123456,
                "lat": 49.8968,
                "lon": 10.8939,
                "tags": {
                    "name": "Denns BioMarkt",
                    "shop": "supermarket",
                    "organic": "only",
                    "addr:street": "Obere Königstraße",
                    "addr:housenumber": "20",
                    "addr:postcode": "96052",
                    "addr:city": "Bamberg",
                    "addr:country": "DE"
                }
            }
        ]
    }

    result = client._parse_response(response_json, 49.8968, 10.8939)

    assert result is not None
    assert result['latitude'] == 49.8968
    assert result['longitude'] == 10.8939
    assert result['name'] == "Denns BioMarkt"
    assert result['osm_id'] == 123456
    # Check address fields
    assert result['street'] == "Obere Königstraße"
    assert result['housenumber'] == "20"
    assert result['postcode'] == "96052"
    assert result['city'] == "Bamberg"
    assert result['country'] == "DE"


def test_calculate_distance_validation():
    """Test Haversine distance calculation for POI validation."""
    client = OverpassClient()

    # Test same point (should be ~0m)
    distance = client._haversine_distance(49.8968, 10.8939, 49.8968, 10.8939)
    assert distance < 1.0  # Less than 1 meter

    # Test points ~25m apart (approximate)
    distance = client._haversine_distance(49.8968, 10.8939, 49.89702, 10.89408)
    assert 20 < distance < 30

    # Test points ~2km apart (Erlangen example from user)
    # Turnstraße: 49.6004, 11.0071
    # Paul-Gossen: 49.57889, 11.00271
    distance = client._haversine_distance(49.6004, 11.0071, 49.57889, 11.00271)
    assert 2300 < distance < 2500  # ~2.4 km


def test_validate_poi_against_scraper():
    """Test POI validation against scraper coordinates."""
    client = OverpassClient()

    # Test valid match (< 100m)
    poi_lat, poi_lon = 49.8968, 10.8939
    scraper_lat, scraper_lon = 49.89702, 10.89408  # ~25m away

    distance, is_valid = client.validate_poi_against_scraper(
        poi_lat, poi_lon, scraper_lat, scraper_lon, threshold=100.0
    )

    assert 20 < distance < 30
    assert is_valid is True

    # Test invalid match (> 100m)
    scraper_lat2, scraper_lon2 = 49.6004, 11.0071  # ~2.4km away

    distance2, is_valid2 = client.validate_poi_against_scraper(
        poi_lat, poi_lon, scraper_lat2, scraper_lon2, threshold=100.0
    )

    assert distance2 > 2000
    assert is_valid2 is False

    # Test exact match
    distance3, is_valid3 = client.validate_poi_against_scraper(
        poi_lat, poi_lon, poi_lat, poi_lon, threshold=100.0
    )

    assert distance3 < 1.0
    assert is_valid3 is True
