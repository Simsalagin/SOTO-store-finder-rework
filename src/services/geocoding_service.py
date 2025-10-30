"""Geocoding service for enriching store data with OSM coordinates."""

import logging
from typing import Dict, List, Any, Optional
from datetime import datetime

try:
    from .nominatim_client import NominatimClient
    from .overpass_client import OverpassClient
    from ..database.db_manager import DatabaseManager
except ImportError:
    from services.nominatim_client import NominatimClient
    from services.overpass_client import OverpassClient
    from database.db_manager import DatabaseManager


logger = logging.getLogger(__name__)


class GeocodingService:
    """Service for geocoding stores using OpenStreetMap Nominatim."""

    def __init__(self, db_path: str = "stores.db"):
        """Initialize geocoding service with 3-tier geocoding strategy.

        Args:
            db_path: Path to SQLite database
        """
        self.db_path = db_path
        self.db = DatabaseManager(db_path)
        self.nominatim = NominatimClient()
        self.overpass = OverpassClient()

    def get_stores_needing_geocoding(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Get stores that need OSM geocoding.

        Args:
            limit: Optional limit on number of stores to return

        Returns:
            List of store dictionaries
        """
        return self.db.get_stores_needing_osm(limit=limit)

    def geocode_store(self, store: Dict[str, Any]) -> bool:
        """Geocode a single store and update database.

        Args:
            store: Store dictionary with address information

        Returns:
            True if geocoding successful, False otherwise
        """
        market_id = store['market_id']

        try:
            # Extract address components
            street = store.get('street')
            city = store.get('city')
            postal_code = store.get('zip')
            country_code = store.get('country_code')

            # Map country code to country name
            country = self._country_code_to_name(country_code)

            # Geocode using Nominatim
            logger.info(f"Geocoding store {market_id}: {city}")
            result = self.nominatim.geocode(
                street=street,
                city=city,
                postal_code=postal_code,
                country=country
            )

            if result:
                # Update store with OSM data
                self.update_store_with_osm_data(market_id, result)
                logger.info(f"Successfully geocoded store {market_id}")
                return True
            else:
                # Mark as checked even though no results found
                self.mark_store_as_checked(market_id)
                logger.warning(f"No geocoding results for store {market_id}")
                return False

        except Exception as e:
            logger.error(f"Error geocoding store {market_id}: {e}")
            # Mark as checked to avoid infinite retries
            self.mark_store_as_checked(market_id)
            return False

    def update_store_with_osm_data(self, market_id: int, osm_data: Dict[str, Any]):
        """Update a store with OSM geocoding data.

        Args:
            market_id: Store market ID
            osm_data: Dictionary with latitude, longitude, display_name
        """
        try:
            cursor = self.db.conn.cursor()
            cursor.execute("""
                UPDATE stores
                SET osm_latitude = ?, osm_longitude = ?, osm_display_name = ?,
                    osm_checked = 1, osm_checked_at = ?
                WHERE market_id = ?
            """, (
                osm_data['latitude'],
                osm_data['longitude'],
                osm_data.get('display_name', ''),
                datetime.now(),
                market_id
            ))
            self.db.conn.commit()
            logger.debug(f"Updated store {market_id} with OSM data")
        except Exception as e:
            logger.error(f"Error updating store {market_id} with OSM data: {e}")
            self.db.conn.rollback()
            raise

    def mark_store_as_checked(self, market_id: int):
        """Mark a store as checked without adding OSM data.

        Args:
            market_id: Store market ID
        """
        try:
            cursor = self.db.conn.cursor()
            cursor.execute("""
                UPDATE stores
                SET osm_checked = 1, osm_checked_at = ?
                WHERE market_id = ?
            """, (datetime.now(), market_id))
            self.db.conn.commit()
            logger.debug(f"Marked store {market_id} as checked")
        except Exception as e:
            logger.error(f"Error marking store {market_id} as checked: {e}")
            self.db.conn.rollback()
            raise

    def geocode_all_stores(self, limit: Optional[int] = None) -> Dict[str, int]:
        """Geocode all stores needing OSM data.

        Args:
            limit: Optional limit on number of stores to process

        Returns:
            Dictionary with statistics (total, successful, failed)
        """
        stores = self.get_stores_needing_geocoding(limit=limit)

        stats = {
            'total': len(stores),
            'successful': 0,
            'failed': 0
        }

        logger.info(f"Starting geocoding for {len(stores)} stores...")

        for i, store in enumerate(stores, 1):
            market_id = store['market_id']
            logger.info(f"Processing store {i}/{len(stores)}: {market_id}")

            success = self.geocode_store(store)

            if success:
                stats['successful'] += 1
            else:
                stats['failed'] += 1

        logger.info(
            f"Geocoding complete: {stats['successful']} successful, "
            f"{stats['failed']} failed out of {stats['total']} total"
        )

        return stats

    def _country_code_to_name(self, country_code: Optional[str]) -> Optional[str]:
        """Convert ISO country code to country name.

        Args:
            country_code: ISO country code (e.g., 'DE', 'FR')

        Returns:
            Country name or None
        """
        if not country_code:
            return None

        # Simple mapping for common European countries
        country_map = {
            'DE': 'Germany',
            'AT': 'Austria',
            'CH': 'Switzerland',
            'FR': 'France',
            'IT': 'Italy',
            'ES': 'Spain',
            'NL': 'Netherlands',
            'BE': 'Belgium',
            'PL': 'Poland',
            'CZ': 'Czech Republic'
        }

        return country_map.get(country_code.upper(), country_code)

    def geocode_store_enhanced(self, store: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Enhanced geocoding with 3-tier strategy and address validation.

        Tier 1: Overpass POI search with validation
        Tier 2: Scraper coordinates
        Tier 3: Nominatim address-level fallback

        Args:
            store: Store dictionary with all fields

        Returns:
            Dictionary with geocoding result:
            {
                'final_latitude': float,
                'final_longitude': float,
                'geocoding_source': str,  # 'overpass_poi', 'scraper', 'nominatim_address'
                'geocoding_confidence': str,  # 'very_high', 'high', 'medium', 'low'
                'tier': int,  # 1, 2, or 3
                'update_address': bool,  # True if address should be updated
                'new_street': str (optional),  # New street from OSM
                'new_zip': str (optional),  # New ZIP from OSM
                'new_city': str (optional)  # New city from OSM
            }
            Returns None if all tiers fail
        """
        name = store['name']
        scraper_lat = store.get('latitude', 0)
        scraper_lon = store.get('longitude', 0)
        scraper_valid = scraper_lat != 0 and scraper_lon != 0

        # TIER 1: Overpass POI Search
        logger.debug(f"Tier 1: Searching Overpass POI for '{name}'")

        # Use scraper coords as search center if available, otherwise use default Germany center
        search_lat = scraper_lat if scraper_valid else 50.0
        search_lon = scraper_lon if scraper_valid else 10.0

        poi_result = self.overpass.search_poi_with_variants(
            name, search_lat, search_lon, radius=100
        )

        if poi_result:
            poi_lat = poi_result['latitude']
            poi_lon = poi_result['longitude']

            if scraper_valid:
                # Validate POI against scraper coordinates
                distance, is_valid = self.overpass.validate_poi_against_scraper(
                    poi_lat, poi_lon, scraper_lat, scraper_lon, threshold=100.0
                )

                if is_valid:
                    # ✅ POI + Scraper validated (< 100m) → very_high
                    logger.info(
                        f"{name}: POI validated by scraper ({distance:.1f}m) → very_high confidence"
                    )
                    return {
                        'final_latitude': poi_lat,
                        'final_longitude': poi_lon,
                        'geocoding_source': 'overpass_poi',
                        'geocoding_confidence': 'very_high',
                        'tier': 1,
                        'update_address': False
                    }
                else:
                    # ⚠️ CONFLICT: POI and Scraper > 100m apart
                    # Trust OSM, update address
                    logger.warning(
                        f"{name}: POI/Scraper distance mismatch ({distance:.1f}m). "
                        f"Using POI coordinates and updating address from OSM. "
                        f"Old address: {store.get('street')}, {store.get('zip')} {store.get('city')} "
                        f"New address: {poi_result.get('street')}, {poi_result.get('postcode')} {poi_result.get('city')}"
                    )

                    result = {
                        'final_latitude': poi_lat,
                        'final_longitude': poi_lon,
                        'geocoding_source': 'overpass_poi',
                        'geocoding_confidence': 'high',
                        'tier': 1,
                        'update_address': True
                    }

                    # Add OSM address data if available
                    if poi_result.get('street'):
                        housenumber = poi_result.get('housenumber', '')
                        street = poi_result['street']
                        result['new_street'] = f"{housenumber} {street}".strip()
                    if poi_result.get('postcode'):
                        result['new_zip'] = poi_result['postcode']
                    if poi_result.get('city'):
                        result['new_city'] = poi_result['city']

                    return result
            else:
                # No scraper validation possible → high
                logger.info(f"{name}: POI found (no scraper validation) → high confidence")
                return {
                    'final_latitude': poi_lat,
                    'final_longitude': poi_lon,
                    'geocoding_source': 'overpass_poi',
                    'geocoding_confidence': 'high',
                    'tier': 1,
                    'update_address': False
                }

        # TIER 2: Scraper Coordinates
        if scraper_valid:
            logger.info(f"{name}: No POI found, using scraper coordinates → medium confidence")
            return {
                'final_latitude': scraper_lat,
                'final_longitude': scraper_lon,
                'geocoding_source': 'scraper',
                'geocoding_confidence': 'medium',
                'tier': 2,
                'update_address': False
            }

        # TIER 3: Nominatim Address-Level Geocoding
        logger.info(f"{name}: No POI or scraper, trying Nominatim → low confidence")

        street = store.get('street')
        city = store.get('city')
        postal_code = store.get('zip')
        country_code = store.get('country_code')
        country = self._country_code_to_name(country_code)

        nominatim_result = self.nominatim.geocode(
            street=street,
            city=city,
            postal_code=postal_code,
            country=country
        )

        if nominatim_result:
            logger.info(f"{name}: Nominatim geocoding successful → low confidence")
            return {
                'final_latitude': nominatim_result['latitude'],
                'final_longitude': nominatim_result['longitude'],
                'geocoding_source': 'nominatim_address',
                'geocoding_confidence': 'low',
                'tier': 3,
                'update_address': False
            }

        # Total failure - no results from any tier
        logger.error(f"{name}: All geocoding tiers failed")
        return None

    def update_store_with_final_coords(self, store_id: int, result: Dict[str, Any]):
        """
        Update store with final coordinates and optionally update address.

        This method updates:
        - final_latitude, final_longitude
        - geocoding_source, geocoding_confidence
        - osm_checked, osm_checked_at
        - Optionally: street, zip, city (if update_address=True)

        Args:
            store_id: Store market_id
            result: Result dictionary from geocode_store_enhanced()
        """
        try:
            cursor = self.db.conn.cursor()

            if result.get('update_address'):
                # Update coordinates AND address
                # Get current values for fields not being updated
                cursor.execute("SELECT street, zip, city FROM stores WHERE market_id = ?", (store_id,))
                current = cursor.fetchone()

                new_street = result.get('new_street', current[0])
                new_zip = result.get('new_zip', current[1])
                new_city = result.get('new_city', current[2])

                cursor.execute("""
                    UPDATE stores
                    SET final_latitude = ?,
                        final_longitude = ?,
                        geocoding_source = ?,
                        geocoding_confidence = ?,
                        street = ?,
                        zip = ?,
                        city = ?,
                        osm_checked = 1,
                        osm_checked_at = ?
                    WHERE market_id = ?
                """, (
                    result['final_latitude'],
                    result['final_longitude'],
                    result['geocoding_source'],
                    result['geocoding_confidence'],
                    new_street,
                    new_zip,
                    new_city,
                    datetime.now(),
                    store_id
                ))
                logger.info(f"Store {store_id}: Updated coordinates and address")
            else:
                # Update only coordinates
                cursor.execute("""
                    UPDATE stores
                    SET final_latitude = ?,
                        final_longitude = ?,
                        geocoding_source = ?,
                        geocoding_confidence = ?,
                        osm_checked = 1,
                        osm_checked_at = ?
                    WHERE market_id = ?
                """, (
                    result['final_latitude'],
                    result['final_longitude'],
                    result['geocoding_source'],
                    result['geocoding_confidence'],
                    datetime.now(),
                    store_id
                ))
                logger.info(f"Store {store_id}: Updated coordinates only")

            self.db.conn.commit()

        except Exception as e:
            logger.error(f"Error updating store {store_id} with final coords: {e}")
            self.db.conn.rollback()
            raise

    def close(self):
        """Close database connection."""
        if self.db:
            self.db.close()

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
