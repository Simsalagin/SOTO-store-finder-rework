"""Geocoding service for enriching store data with OSM coordinates."""

import logging
from typing import Dict, List, Any, Optional
from datetime import datetime

try:
    from .nominatim_client import NominatimClient
    from ..database.db_manager import DatabaseManager
except ImportError:
    from services.nominatim_client import NominatimClient
    from database.db_manager import DatabaseManager


logger = logging.getLogger(__name__)


class GeocodingService:
    """Service for geocoding stores using OpenStreetMap Nominatim."""

    def __init__(self, db_path: str = "stores.db"):
        """Initialize geocoding service.

        Args:
            db_path: Path to SQLite database
        """
        self.db_path = db_path
        self.db = DatabaseManager(db_path)
        self.nominatim = NominatimClient()

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
