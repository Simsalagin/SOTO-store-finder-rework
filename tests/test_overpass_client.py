"""
Tests for OSM Overpass API client for POI search.

This module tests the Overpass API client that searches for specific business POIs
(Points of Interest) in OpenStreetMap data, providing exact and fuzzy matching
within a specified radius.

Following TDD: These tests are written first (RED phase) before implementation.
"""

import pytest
import responses
from responses import matchers
import json


def test_overpass_client_import():
    """Test that OverpassClient can be imported."""
    from src.services.overpass_client import OverpassClient

    client = OverpassClient()
    assert client is not None
    assert hasattr(client, 'search_poi_exact')
    assert hasattr(client, 'search_poi_fuzzy')


@responses.activate
def test_poi_search_exact_match_success():
    """Test searching for exact POI name match returns coordinates."""
    from src.services.overpass_client import OverpassClient

    # Mock Overpass API response with a single POI
    mock_response = {
        "version": 0.6,
        "elements": [
            {
                "type": "node",
                "id": 123456789,
                "lat": 51.3319,
                "lon": 6.5623,
                "tags": {
                    "name": "Denns BioMarkt",
                    "shop": "supermarket",
                    "organic": "only"
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

    client = OverpassClient()
    result = client.search_poi_exact("Denns BioMarkt", 51.3320, 6.5620, radius=100)

    assert result is not None
    assert result['latitude'] == 51.3319
    assert result['longitude'] == 6.5623
    assert result['name'] == "Denns BioMarkt"
    assert result['osm_id'] == 123456789


@responses.activate
def test_poi_search_fuzzy_match_success():
    """Test fuzzy POI search with name pattern and shop tag."""
    from src.services.overpass_client import OverpassClient

    # Mock Overpass API response with fuzzy match
    mock_response = {
        "version": 0.6,
        "elements": [
            {
                "type": "node",
                "id": 987654321,
                "lat": 51.3319,
                "lon": 6.5623,
                "tags": {
                    "name": "Denns BioMarkt Krefeld",
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

    client = OverpassClient()
    result = client.search_poi_fuzzy("Denns", 51.3320, 6.5620, radius=100)

    assert result is not None
    assert result['latitude'] == 51.3319
    assert result['longitude'] == 6.5623
    assert 'Denns' in result['name']


@responses.activate
def test_poi_search_no_results():
    """Test that search returns None when no POI is found."""
    from src.services.overpass_client import OverpassClient

    # Mock empty response
    mock_response = {
        "version": 0.6,
        "elements": []
    }

    responses.add(
        responses.POST,
        "https://overpass-api.de/api/interpreter",
        json=mock_response,
        status=200
    )

    client = OverpassClient()
    result = client.search_poi_exact("Nonexistent Store", 51.3320, 6.5620, radius=100)

    assert result is None


@responses.activate
def test_multiple_pois_pick_closest():
    """Test that when multiple POIs are found, the closest one is returned."""
    from src.services.overpass_client import OverpassClient

    # Mock response with 3 POIs at different distances
    mock_response = {
        "version": 0.6,
        "elements": [
            {
                "type": "node",
                "id": 1,
                "lat": 51.3340,  # ~220m away
                "lon": 6.5640,
                "tags": {"name": "Denns BioMarkt"}
            },
            {
                "type": "node",
                "id": 2,
                "lat": 51.3322,  # ~25m away (closest)
                "lon": 6.5622,
                "tags": {"name": "Denns BioMarkt"}
            },
            {
                "type": "node",
                "id": 3,
                "lat": 51.3310,  # ~110m away
                "lon": 6.5610,
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

    client = OverpassClient()
    result = client.search_poi_exact("Denns BioMarkt", 51.3320, 6.5620, radius=100)

    assert result is not None
    assert result['osm_id'] == 2  # Closest POI
    assert result['latitude'] == 51.3322
    assert result['longitude'] == 6.5622


def test_query_construction_100m_radius():
    """Test that Overpass QL query is constructed correctly with 100m radius."""
    from src.services.overpass_client import OverpassClient

    client = OverpassClient()

    # Test exact match query construction
    query = client._build_query('node["name"="Denns BioMarkt"]', 51.3320, 6.5620, 100)

    assert 'node["name"="Denns BioMarkt"]' in query
    assert '(around:100,51.332,6.562)' in query
    assert 'out body;' in query

    # Test fuzzy match query construction
    query_fuzzy = client._build_query('node["name"~"Denns"]["shop"]', 51.3320, 6.5620, 100)

    assert 'node["name"~"Denns"]["shop"]' in query_fuzzy
    assert '(around:100,51.332,6.562)' in query_fuzzy


def test_parse_overpass_json_response():
    """Test parsing Overpass JSON response to extract POI data."""
    from src.services.overpass_client import OverpassClient

    client = OverpassClient()

    response_json = {
        "version": 0.6,
        "elements": [
            {
                "type": "node",
                "id": 123456,
                "lat": 51.3319,
                "lon": 6.5623,
                "tags": {
                    "name": "Denns BioMarkt",
                    "shop": "supermarket"
                }
            }
        ]
    }

    result = client._parse_response(response_json, 51.3320, 6.5620)

    assert result is not None
    assert result['latitude'] == 51.3319
    assert result['longitude'] == 6.5623
    assert result['name'] == "Denns BioMarkt"
    assert result['osm_id'] == 123456


@responses.activate
def test_handle_timeout_errors():
    """Test graceful handling of request timeout errors."""
    from src.services.overpass_client import OverpassClient
    import requests

    responses.add(
        responses.POST,
        "https://overpass-api.de/api/interpreter",
        body=requests.exceptions.Timeout("Request timed out")
    )

    client = OverpassClient()
    result = client.search_poi_exact("Denns BioMarkt", 51.3320, 6.5620, radius=100)

    # Should return None on timeout, not raise exception
    assert result is None


@responses.activate
def test_handle_connection_errors():
    """Test graceful handling of network connection errors."""
    from src.services.overpass_client import OverpassClient
    import requests

    responses.add(
        responses.POST,
        "https://overpass-api.de/api/interpreter",
        body=requests.exceptions.ConnectionError("Network unreachable")
    )

    client = OverpassClient()
    result = client.search_poi_exact("Denns BioMarkt", 51.3320, 6.5620, radius=100)

    # Should return None on connection error, not raise exception
    assert result is None


@responses.activate
def test_handle_http_errors():
    """Test handling of HTTP error status codes (429, 500, etc.)."""
    from src.services.overpass_client import OverpassClient

    # Test 429 Too Many Requests
    responses.add(
        responses.POST,
        "https://overpass-api.de/api/interpreter",
        json={"error": "rate limit exceeded"},
        status=429
    )

    client = OverpassClient()
    result = client.search_poi_exact("Denns BioMarkt", 51.3320, 6.5620, radius=100)

    # Should return None on HTTP error, not raise exception
    assert result is None


def test_track_query_count_10k_limit():
    """Test that query count is tracked for 10k daily limit."""
    from src.services.overpass_client import OverpassClient

    client = OverpassClient()

    assert hasattr(client, 'query_count')
    assert hasattr(client, 'daily_limit')
    assert client.daily_limit == 10000
    assert client.query_count == 0
