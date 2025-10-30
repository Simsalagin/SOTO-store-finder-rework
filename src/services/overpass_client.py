"""
OpenStreetMap Overpass API client for POI (Point of Interest) search.

This module provides functionality to search for specific business POIs in OpenStreetMap
using the Overpass API. It supports exact and fuzzy name matching within a specified
radius to find actual store locations rather than just address-level coordinates.

Usage:
    client = OverpassClient()
    result = client.search_poi_exact("Denns BioMarkt", 51.3320, 6.5620, radius=100)
    if result:
        print(f"Found POI at {result['latitude']}, {result['longitude']}")
"""

import requests
import logging
import math
import time
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)


class OverpassClient:
    """Client for querying OSM Overpass API to find POIs."""

    def __init__(self, delay_between_requests: float = 1.5):
        """
        Initialize Overpass API client with rate limiting.

        Args:
            delay_between_requests: Minimum seconds between requests (default: 1.5)
        """
        self.base_url = "https://overpass-api.de/api/interpreter"
        self.query_count = 0
        self.daily_limit = 10000
        self.timeout = 30  # seconds
        self.delay_between_requests = delay_between_requests
        self.last_request_time = 0

    def search_poi_exact(self, name: str, lat: float, lon: float, radius: int = 100) -> Optional[Dict[str, Any]]:
        """
        Search for POI with exact name match within radius.

        Args:
            name: Exact store name to search for (e.g., "Denns BioMarkt")
            lat: Latitude of center point
            lon: Longitude of center point
            radius: Search radius in meters (default: 100)

        Returns:
            Dictionary with POI data if found, None otherwise:
            {
                'latitude': float,
                'longitude': float,
                'name': str,
                'osm_id': int
            }
        """
        query_filter = f'node["name"="{name}"]'
        return self._execute_search(query_filter, lat, lon, radius)

    def search_poi_fuzzy(self, name_pattern: str, lat: float, lon: float, radius: int = 100) -> Optional[Dict[str, Any]]:
        """
        Search for POI with fuzzy name match and shop tag within radius.

        Uses regex matching on name and requires shop tag for more targeted results.

        Args:
            name_pattern: Name pattern to search for (e.g., "Denns" matches "Denns BioMarkt Krefeld")
            lat: Latitude of center point
            lon: Longitude of center point
            radius: Search radius in meters (default: 100)

        Returns:
            Dictionary with POI data if found, None otherwise
        """
        # Use regex for fuzzy matching and require shop tag
        query_filter = f'node["name"~"{name_pattern}"]["shop"]'
        return self._execute_search(query_filter, lat, lon, radius)

    def _execute_search(self, query_filter: str, lat: float, lon: float, radius: int) -> Optional[Dict[str, Any]]:
        """
        Execute Overpass API search with given query filter.

        Args:
            query_filter: Overpass QL filter expression
            lat: Latitude of center point
            lon: Longitude of center point
            radius: Search radius in meters

        Returns:
            Dictionary with POI data if found, None otherwise
        """
        # Check rate limit
        if self.query_count >= self.daily_limit:
            logger.warning(f"Daily query limit reached ({self.daily_limit})")
            return None

        # Rate limiting: wait before making request if needed
        if self.last_request_time > 0:
            time_since_last = time.time() - self.last_request_time
            if time_since_last < self.delay_between_requests:
                sleep_time = self.delay_between_requests - time_since_last
                logger.debug(f"Rate limiting: sleeping {sleep_time:.2f}s")
                time.sleep(sleep_time)

        # Build Overpass QL query
        query = self._build_query(query_filter, lat, lon, radius)

        try:
            # Make POST request to Overpass API
            self.last_request_time = time.time()
            response = requests.post(
                self.base_url,
                data={"data": query},
                timeout=self.timeout
            )

            # Increment query count
            self.query_count += 1

            # Check HTTP status
            if response.status_code != 200:
                if response.status_code == 429:
                    logger.warning("Overpass API rate limit exceeded (429) - increase delay_between_requests")
                elif response.status_code == 504:
                    logger.warning("Overpass API gateway timeout (504) - server overloaded, try again later")
                else:
                    logger.warning(f"Overpass API returned status {response.status_code}")
                return None

            # Parse JSON response
            response_json = response.json()
            return self._parse_response(response_json, lat, lon)

        except requests.exceptions.Timeout:
            logger.warning(f"Overpass API request timed out after {self.timeout}s")
            return None

        except requests.exceptions.ConnectionError as e:
            logger.warning(f"Connection error to Overpass API: {e}")
            return None

        except requests.exceptions.RequestException as e:
            logger.warning(f"Request error to Overpass API: {e}")
            return None

        except Exception as e:
            logger.error(f"Unexpected error in Overpass search: {e}")
            return None

    def _build_query(self, query_filter: str, lat: float, lon: float, radius: int) -> str:
        """
        Build Overpass QL query string.

        Args:
            query_filter: Filter expression (e.g., 'node["name"="Denns BioMarkt"]')
            lat: Latitude of center point
            lon: Longitude of center point
            radius: Search radius in meters

        Returns:
            Complete Overpass QL query string
        """
        # Overpass QL format:
        # [out:json];
        # node["name"="Denns BioMarkt"](around:100,51.332,6.562);
        # out body;
        query = f"""[out:json];
{query_filter}(around:{radius},{lat},{lon});
out body;
"""
        return query

    def _parse_response(self, response_json: Dict[str, Any], center_lat: float, center_lon: float) -> Optional[Dict[str, Any]]:
        """
        Parse Overpass JSON response and return closest POI.

        When multiple POIs are found, returns the one closest to the center point.

        Args:
            response_json: JSON response from Overpass API
            center_lat: Latitude of search center (for distance calculation)
            center_lon: Longitude of search center (for distance calculation)

        Returns:
            Dictionary with closest POI data, or None if no elements found
        """
        elements = response_json.get('elements', [])

        if not elements:
            return None

        # Helper function to extract POI data with address tags
        def extract_poi_data(element):
            tags = element.get('tags', {})
            return {
                'latitude': element['lat'],
                'longitude': element['lon'],
                'name': tags.get('name', 'Unknown'),
                'osm_id': element['id'],
                # Extract address tags from OSM
                'street': tags.get('addr:street'),
                'housenumber': tags.get('addr:housenumber'),
                'postcode': tags.get('addr:postcode'),
                'city': tags.get('addr:city'),
                'country': tags.get('addr:country')
            }

        # If single POI, return it
        if len(elements) == 1:
            return extract_poi_data(elements[0])

        # Multiple POIs - find closest one
        closest_poi = None
        min_distance = float('inf')

        for element in elements:
            poi_lat = element['lat']
            poi_lon = element['lon']

            # Calculate distance using haversine formula
            distance = self._haversine_distance(center_lat, center_lon, poi_lat, poi_lon)

            if distance < min_distance:
                min_distance = distance
                closest_poi = extract_poi_data(element)

        return closest_poi

    def _haversine_distance(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """
        Calculate distance between two points using Haversine formula.

        Args:
            lat1: Latitude of first point
            lon1: Longitude of first point
            lat2: Latitude of second point
            lon2: Longitude of second point

        Returns:
            Distance in meters
        """
        # Earth radius in meters
        R = 6371000

        # Convert to radians
        lat1_rad = math.radians(lat1)
        lat2_rad = math.radians(lat2)
        delta_lat = math.radians(lat2 - lat1)
        delta_lon = math.radians(lon2 - lon1)

        # Haversine formula
        a = (math.sin(delta_lat / 2) ** 2 +
             math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon / 2) ** 2)
        c = 2 * math.asin(math.sqrt(a))

        return R * c

    def _extract_base_name(self, full_name: str) -> str:
        """
        Extract base store name by removing city suffix.

        Removes the last word from store name which is typically the city.
        For example: "Denns BioMarkt Erlangen" → "Denns BioMarkt"

        Args:
            full_name: Full store name including city

        Returns:
            Base store name without city suffix
        """
        if not full_name:
            return ""

        parts = full_name.strip().split()

        # Only remove last word if there are more than 2 words
        # This prevents "Speisekammer Hof" from becoming just "Speisekammer"
        if len(parts) > 2:
            return ' '.join(parts[:-1])

        return full_name

    def search_poi_with_variants(self, store_name: str, lat: float, lon: float, radius: int = 100) -> Optional[Dict[str, Any]]:
        """
        Search for POI with multiple name variants.

        Tries multiple exact and fuzzy name patterns to find the POI:
        1. Exact: "Denns BioMarkt" (city removed)
        2. Exact: "Denns Biomarkt" (lowercase 'i' in Bio)
        3. Exact: "denn's Biomarkt" (with apostrophe)
        4. Fuzzy: "denn's" (with shop tag)
        5. Fuzzy: "Denns" (with shop tag)

        Stops at first successful match.

        Args:
            store_name: Full store name (e.g., "Denns BioMarkt Erlangen")
            lat: Latitude for search center
            lon: Longitude for search center
            radius: Search radius in meters (default: 100)

        Returns:
            POI data with 'matched_variant' field, or None if not found
        """
        # Extract base name (remove city)
        base_name = self._extract_base_name(store_name)

        # Define exact match variants
        exact_variants = [
            base_name,  # "Denns BioMarkt"
            base_name.replace('BioMarkt', 'Biomarkt'),  # "Denns Biomarkt"
            base_name.replace('Denns', "denn's")  # "denn's BioMarkt"
        ]

        # Try exact matches first
        for variant in exact_variants:
            logger.debug(f"Trying exact match: '{variant}'")
            result = self.search_poi_exact(variant, lat, lon, radius)
            if result:
                result['matched_variant'] = variant
                logger.info(f"Found POI with exact match: '{variant}'")
                return result

        # Define fuzzy patterns
        fuzzy_patterns = ["denn's", "Denns"]

        # Try fuzzy matches
        for pattern in fuzzy_patterns:
            logger.debug(f"Trying fuzzy match: '{pattern}'")
            result = self.search_poi_fuzzy(pattern, lat, lon, radius)
            if result:
                result['matched_variant'] = f"fuzzy:{pattern}"
                logger.info(f"Found POI with fuzzy match: '{pattern}'")
                return result

        logger.debug(f"No POI found for any variant of '{store_name}'")
        return None

    def validate_poi_against_scraper(self, poi_lat: float, poi_lon: float,
                                    scraper_lat: float, scraper_lon: float,
                                    threshold: float = 100.0) -> tuple:
        """
        Validate POI coordinates against scraper coordinates.

        Calculates the distance between POI and scraper coordinates to determine
        if they represent the same location.

        Args:
            poi_lat: POI latitude from Overpass
            poi_lon: POI longitude from Overpass
            scraper_lat: Scraper latitude from database
            scraper_lon: Scraper longitude from database
            threshold: Maximum distance in meters to consider valid (default: 100m)

        Returns:
            Tuple of (distance_meters, is_valid)
            - distance_meters: Distance between coordinates in meters
            - is_valid: True if distance < threshold, False otherwise
        """
        distance = self._haversine_distance(poi_lat, poi_lon, scraper_lat, scraper_lon)
        is_valid = distance < threshold

        logger.debug(
            f"POI validation: distance={distance:.1f}m, threshold={threshold}m, valid={is_valid}"
        )

        return (distance, is_valid)
