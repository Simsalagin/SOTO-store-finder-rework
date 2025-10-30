"""Tests for Biomarkt scraper."""

import pytest
from scrapers.biomarkt_scraper import BiomarktScraper


def test_scraper_initialization():
    """Test scraper initialization."""
    scraper = BiomarktScraper()
    assert scraper.name == 'biomarkt'
    assert scraper.URL is not None


def test_parse_store_node():
    """Test parsing individual store node."""
    scraper = BiomarktScraper()

    sample_node = {
        'marketId': 12345,
        'name': 'Test Store',
        'countryCode': 'DE',
        'status': 'active',
        'openingDay': '2020-01-01',
        'isLoyaltyMarket': True,
        'address': {
            'street': 'Test Street 1',
            'zip': '12345',
            'city': 'Test City',
            'lat': 52.5200,
            'lon': 13.4050,
            'googleProfileLink': 'https://maps.google.com/test'
        },
        'contact': {
            'phone': '+49 123 456789',
            'email': 'test@example.com'
        },
        'openingHoursMarket': [
            {
                'weekday': 'Monday',
                'open_from': '09:00',
                'open_until': '18:00'
            }
        ]
    }

    store = scraper._parse_store(sample_node)

    assert store['market_id'] == 12345
    assert store['name'] == 'Test Store'
    assert store['street'] == 'Test Street 1'
    assert store['city'] == 'Test City'
    assert store['latitude'] == 52.5200
    assert store['longitude'] == 13.4050
    assert len(store['opening_hours']) == 1


def test_parse_store_with_missing_fields():
    """Test parsing store with missing optional fields."""
    scraper = BiomarktScraper()

    sample_node = {
        'marketId': 12345,
        'name': 'Test Store',
        'address': {},
        'contact': {}
    }

    store = scraper._parse_store(sample_node)

    assert store['market_id'] == 12345
    assert store['name'] == 'Test Store'
    assert store['street'] is None
    assert store['phone'] is None


def test_coordinate_cleaning():
    """Test coordinate validation and cleaning."""
    scraper = BiomarktScraper()

    # Valid coordinates
    sample_node = {
        'marketId': 12345,
        'name': 'Test Store',
        'address': {
            'lat': 52.5200,
            'lon': 13.4050
        },
        'contact': {}
    }

    store = scraper._parse_store(sample_node)
    assert store['latitude'] == 52.5200
    assert store['longitude'] == 13.4050

    # Invalid coordinates
    sample_node['address'] = {'lat': 'invalid', 'lon': 'invalid'}
    store = scraper._parse_store(sample_node)
    assert store['latitude'] is None
    assert store['longitude'] is None


# Integration test (requires network)
@pytest.mark.integration
def test_fetch_real_data():
    """Test fetching real data from Biomarkt API.

    This is an integration test and requires network access.
    """
    scraper = BiomarktScraper()

    # Fetch data
    raw_data = scraper.fetch_data()
    assert raw_data is not None
    assert 'result' in raw_data

    # Parse data
    stores = scraper.parse_data(raw_data)
    assert len(stores) > 0
    assert all('market_id' in store for store in stores)
    assert all('name' in store for store in stores)


# Integration test (requires network)
@pytest.mark.integration
def test_full_scrape():
    """Test complete scraping workflow.

    This is an integration test and requires network access.
    """
    scraper = BiomarktScraper()
    stores = scraper.scrape()

    assert len(stores) > 500  # Should have around 596 stores
    assert all('market_id' in store for store in stores)

    # Check for duplicates
    market_ids = [s['market_id'] for s in stores]
    assert len(market_ids) == len(set(market_ids)), "Duplicate market_ids found"
