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

    # After filtering (status '4' stores removed), expect ~515 stores
    # (495 active status='9' + ~20 upcoming status='5')
    assert len(stores) > 490, f"Expected > 490 stores, got {len(stores)}"
    assert all('market_id' in store for store in stores)

    # Check for duplicates
    market_ids = [s['market_id'] for s in stores]
    assert len(market_ids) == len(set(market_ids)), "Duplicate market_ids found"


@pytest.mark.integration
def test_status_filtering():
    """Test that status filtering works correctly.

    This test verifies that the scraper filters out stores based on
    the frontend logic from biomarkt.de/marktindex:
    - Status '4' stores should be filtered out (old/closed versions)
    - Status '5' and '9' stores should be included
    """
    scraper = BiomarktScraper()

    # Fetch and parse data
    raw_data = scraper.fetch_data()
    stores = scraper.parse_data(raw_data)

    # Check that no status='4' stores are included
    status_4_stores = [s for s in stores if s.get('status') == '4']
    assert len(status_4_stores) == 0, (
        f"Found {len(status_4_stores)} status='4' stores, expected 0. "
        f"These should have been filtered out."
    )

    # Verify that the problematic market_id='1' (Turnstraße 9) is filtered out
    market_id_1 = [s for s in stores if s.get('market_id') == 1]
    assert len(market_id_1) == 0, (
        "market_id=1 (Turnstraße 9, Erlangen) should be filtered out "
        "(status='4')"
    )

    # Check that we have both status='5' and status='9' stores
    status_5_stores = [s for s in stores if s.get('status') == '5']
    status_9_stores = [s for s in stores if s.get('status') == '9']

    assert len(status_9_stores) > 450, (
        f"Expected > 450 status='9' stores, got {len(status_9_stores)}"
    )
    assert len(status_5_stores) > 0, (
        f"Expected some status='5' stores, got {len(status_5_stores)}"
    )
