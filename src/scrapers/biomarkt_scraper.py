"""Biomarkt store scraper implementation."""

import requests
from typing import List, Dict, Any

try:
    from .base_scraper import BaseScraper
    from ..utils.validators import validate_store_data, clean_coordinates
except ImportError:
    from scrapers.base_scraper import BaseScraper
    from utils.validators import validate_store_data, clean_coordinates


class BiomarktScraper(BaseScraper):
    """Scraper for Biomarkt store data."""

    URL = "https://www.biomarkt.de/page-data/marktindex/page-data.json"

    def __init__(self):
        """Initialize Biomarkt scraper."""
        super().__init__("biomarkt")

    def fetch_data(self) -> Dict[str, Any]:
        """Fetch store data from Biomarkt API.

        Returns:
            JSON response data

        Raises:
            requests.RequestException: If request fails
        """
        try:
            response = requests.get(self.URL, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            self.logger.error(f"Failed to fetch data: {e}")
            raise

    def parse_data(self, raw_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Parse Biomarkt JSON data into standardized format.

        Args:
            raw_data: Raw JSON response

        Returns:
            List of standardized store dictionaries
        """
        stores = []

        try:
            # Navigate to stores list
            nodes = raw_data['result']['data']['markets']['nodes']
            self.logger.info(f"Found {len(nodes)} stores in response")

            for node in nodes:
                try:
                    store_data = self._parse_store(node)
                    if validate_store_data(store_data):
                        stores.append(store_data)
                    else:
                        self.logger.warning(
                            f"Skipping invalid store: {node.get('marketId')}"
                        )
                except Exception as e:
                    self.logger.error(
                        f"Error parsing store {node.get('marketId')}: {e}"
                    )
                    continue

        except KeyError as e:
            self.logger.error(f"Unexpected JSON structure: {e}")
            raise

        return stores

    def _parse_store(self, node: Dict[str, Any]) -> Dict[str, Any]:
        """Parse individual store node.

        Args:
            node: Store node from JSON

        Returns:
            Standardized store dictionary
        """
        address = node.get('address', {})
        contact = node.get('contact', {})

        # Clean coordinates
        lat, lon = clean_coordinates(
            address.get('lat'),
            address.get('lon')
        )

        # Extract opening hours
        opening_hours = []
        hours_data = node.get('openingHoursMarket', [])
        if isinstance(hours_data, list):
            for hour in hours_data:
                opening_hours.append({
                    'weekday': hour.get('weekday'),
                    'open_from': hour.get('open_from'),
                    'open_until': hour.get('open_until')
                })

        store = {
            'market_id': node.get('marketId'),
            'name': node.get('name'),
            'street': address.get('street'),
            'zip': address.get('zip'),
            'city': address.get('city'),
            'latitude': lat,
            'longitude': lon,
            'phone': contact.get('phone'),
            'email': contact.get('email'),
            'country_code': node.get('countryCode'),
            'status': node.get('status'),
            'opening_day': node.get('openingDay'),
            'is_loyalty_market': node.get('isLoyaltyMarket'),
            'google_maps_link': address.get('googleProfileLink'),
            'opening_hours': opening_hours
        }

        return store
