"""Tests for Nominatim API client."""

import time
import pytest
import responses
from requests.exceptions import Timeout, ConnectionError


def test_nominatim_import():
    """Test that we can import the NominatimClient."""
    from services.nominatim_client import NominatimClient

    client = NominatimClient()
    assert client is not None


@responses.activate
def test_geocode_success():
    """Test successful geocoding of an address."""
    from services.nominatim_client import NominatimClient

    # Mock successful Nominatim response
    responses.add(
        responses.GET,
        'https://nominatim.openstreetmap.org/search',
        json=[{
            'lat': '52.5200',
            'lon': '13.4050',
            'display_name': 'Berlin, Deutschland'
        }],
        status=200
    )

    client = NominatimClient()
    result = client.geocode(
        street='Unter den Linden 1',
        city='Berlin',
        postal_code='10117',
        country='Germany'
    )

    assert result is not None
    assert result['latitude'] == 52.5200
    assert result['longitude'] == 13.4050
    assert result['display_name'] == 'Berlin, Deutschland'


@responses.activate
def test_geocode_no_results():
    """Test geocoding when no results are found."""
    from services.nominatim_client import NominatimClient

    # Mock empty response (no results found)
    responses.add(
        responses.GET,
        'https://nominatim.openstreetmap.org/search',
        json=[],
        status=200
    )

    client = NominatimClient()
    result = client.geocode(
        street='Nonexistent Street 999',
        city='Imaginary City',
        postal_code='00000',
        country='Nowhere'
    )

    assert result is None


@responses.activate
def test_geocode_builds_correct_url():
    """Test that the API request URL is built correctly."""
    from services.nominatim_client import NominatimClient

    responses.add(
        responses.GET,
        'https://nominatim.openstreetmap.org/search',
        json=[{'lat': '50.0', 'lon': '10.0', 'display_name': 'Test'}],
        status=200
    )

    client = NominatimClient()
    client.geocode(
        street='Test Street 1',
        city='Test City',
        postal_code='12345',
        country='Germany'
    )

    # Check the request was made with correct parameters
    assert len(responses.calls) == 1
    request = responses.calls[0].request
    assert 'format=json' in request.url
    assert 'Test+Street+1' in request.url or 'Test%20Street%201' in request.url
    assert 'Test+City' in request.url or 'Test%20City' in request.url
    assert '12345' in request.url
    assert 'Germany' in request.url


@responses.activate
def test_geocode_handles_timeout():
    """Test handling of timeout errors."""
    from services.nominatim_client import NominatimClient

    responses.add(
        responses.GET,
        'https://nominatim.openstreetmap.org/search',
        body=Timeout()
    )

    client = NominatimClient()
    result = client.geocode(
        street='Test Street',
        city='Test City',
        postal_code='12345',
        country='Germany'
    )

    assert result is None


@responses.activate
def test_geocode_handles_connection_error():
    """Test handling of connection errors."""
    from services.nominatim_client import NominatimClient

    responses.add(
        responses.GET,
        'https://nominatim.openstreetmap.org/search',
        body=ConnectionError()
    )

    client = NominatimClient()
    result = client.geocode(
        street='Test Street',
        city='Test City',
        postal_code='12345',
        country='Germany'
    )

    assert result is None


@responses.activate
def test_geocode_handles_http_error():
    """Test handling of HTTP errors (500, 503, etc.)."""
    from services.nominatim_client import NominatimClient

    responses.add(
        responses.GET,
        'https://nominatim.openstreetmap.org/search',
        json={'error': 'Internal server error'},
        status=500
    )

    client = NominatimClient()
    result = client.geocode(
        street='Test Street',
        city='Test City',
        postal_code='12345',
        country='Germany'
    )

    assert result is None


def test_rate_limiting():
    """Test that rate limiting enforces 1 request per second."""
    from services.nominatim_client import NominatimClient

    client = NominatimClient()

    # Make first request
    start_time = time.time()
    client._wait_for_rate_limit()

    # Make second request
    client._wait_for_rate_limit()
    elapsed = time.time() - start_time

    # Should have waited at least 1 second
    assert elapsed >= 1.0, f"Rate limiting failed: only {elapsed} seconds elapsed"


@responses.activate
def test_user_agent_is_set():
    """Test that a proper User-Agent header is set."""
    from services.nominatim_client import NominatimClient

    responses.add(
        responses.GET,
        'https://nominatim.openstreetmap.org/search',
        json=[{'lat': '50.0', 'lon': '10.0', 'display_name': 'Test'}],
        status=200
    )

    client = NominatimClient()
    client.geocode(street='Test', city='Test', postal_code='12345', country='Germany')

    # Check User-Agent header
    assert len(responses.calls) == 1
    request = responses.calls[0].request
    assert 'User-Agent' in request.headers
    assert 'SOTO' in request.headers['User-Agent'] or 'Store' in request.headers['User-Agent']


@responses.activate
def test_geocode_with_minimal_data():
    """Test geocoding with only city (no street or postal code)."""
    from services.nominatim_client import NominatimClient

    responses.add(
        responses.GET,
        'https://nominatim.openstreetmap.org/search',
        json=[{'lat': '48.1351', 'lon': '11.5820', 'display_name': 'München, Deutschland'}],
        status=200
    )

    client = NominatimClient()
    result = client.geocode(
        street=None,
        city='München',
        postal_code=None,
        country='Germany'
    )

    assert result is not None
    assert result['latitude'] == 48.1351
    assert result['longitude'] == 11.5820
