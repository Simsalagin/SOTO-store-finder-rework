"""Nominatim API client for geocoding addresses."""

import time
import logging
from typing import Optional, Dict, Any
import requests
from requests.exceptions import RequestException, Timeout, ConnectionError


logger = logging.getLogger(__name__)


class NominatimClient:
    """Client for OpenStreetMap Nominatim geocoding API."""

    BASE_URL = "https://nominatim.openstreetmap.org/search"
    USER_AGENT = "SOTO Store Finder/1.0"

    def __init__(self):
        """Initialize the Nominatim client."""
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': self.USER_AGENT
        })
        self.last_request_time = 0

    def _wait_for_rate_limit(self):
        """Enforce 1 request per second rate limit."""
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time

        if time_since_last_request < 1.0:
            sleep_time = 1.0 - time_since_last_request
            logger.debug(f"Rate limiting: sleeping for {sleep_time:.2f} seconds")
            time.sleep(sleep_time)

        self.last_request_time = time.time()

    def _build_query(
        self,
        street: Optional[str],
        city: Optional[str],
        postal_code: Optional[str],
        country: Optional[str]
    ) -> str:
        """Build search query from address components.

        Args:
            street: Street address
            city: City name
            postal_code: Postal/ZIP code
            country: Country name

        Returns:
            Formatted search query string
        """
        parts = []

        if street:
            parts.append(street)
        if postal_code:
            parts.append(postal_code)
        if city:
            parts.append(city)
        if country:
            parts.append(country)

        return ', '.join(parts)

    def geocode(
        self,
        street: Optional[str] = None,
        city: Optional[str] = None,
        postal_code: Optional[str] = None,
        country: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """Geocode an address using Nominatim API.

        Args:
            street: Street address
            city: City name
            postal_code: Postal/ZIP code
            country: Country name

        Returns:
            Dictionary with latitude, longitude, and display_name, or None if not found
        """
        # Wait for rate limit
        self._wait_for_rate_limit()

        # Build query
        query = self._build_query(street, city, postal_code, country)

        if not query:
            logger.warning("No address components provided for geocoding")
            return None

        # Make request
        params = {
            'q': query,
            'format': 'json',
            'limit': 1,
            'addressdetails': 0
        }

        try:
            logger.debug(f"Geocoding: {query}")
            response = self.session.get(
                self.BASE_URL,
                params=params,
                timeout=10
            )

            # Check for HTTP errors
            if response.status_code != 200:
                logger.error(f"Nominatim API error: HTTP {response.status_code}")
                return None

            # Parse response
            data = response.json()

            if not data or len(data) == 0:
                logger.info(f"No results found for: {query}")
                return None

            # Extract first result
            result = data[0]

            return {
                'latitude': float(result['lat']),
                'longitude': float(result['lon']),
                'display_name': result.get('display_name', '')
            }

        except Timeout:
            logger.error(f"Timeout while geocoding: {query}")
            return None
        except ConnectionError:
            logger.error(f"Connection error while geocoding: {query}")
            return None
        except RequestException as e:
            logger.error(f"Request error while geocoding {query}: {e}")
            return None
        except (KeyError, ValueError, TypeError) as e:
            logger.error(f"Error parsing Nominatim response for {query}: {e}")
            return None
