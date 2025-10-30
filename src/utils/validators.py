"""Data validation utilities."""

import logging
from typing import Dict, Any, Optional


logger = logging.getLogger(__name__)


def validate_store_data(store: Dict[str, Any]) -> bool:
    """Validate store data has required fields.

    Args:
        store: Store data dictionary

    Returns:
        True if valid, False otherwise
    """
    required_fields = ['market_id', 'name']

    for field in required_fields:
        if field not in store or store[field] is None:
            logger.warning(f"Store missing required field: {field}")
            return False

    # Validate market_id is numeric
    try:
        int(store['market_id'])
    except (ValueError, TypeError):
        logger.warning(f"Invalid market_id: {store.get('market_id')}")
        return False

    return True


def clean_coordinates(lat: Optional[float], lon: Optional[float]) -> tuple:
    """Clean and validate coordinates.

    Args:
        lat: Latitude
        lon: Longitude

    Returns:
        Tuple of (latitude, longitude) or (None, None) if invalid
    """
    try:
        if lat is not None and lon is not None:
            lat_f = float(lat)
            lon_f = float(lon)
            # Basic validation for European coordinates
            if -90 <= lat_f <= 90 and -180 <= lon_f <= 180:
                return lat_f, lon_f
    except (ValueError, TypeError):
        pass

    return None, None
