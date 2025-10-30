"""Database manager for store data."""

import sqlite3
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime

try:
    from .models import STORES_TABLE_SQL, OPENING_HOURS_TABLE_SQL, CREATE_INDEX_SQL
except ImportError:
    from database.models import STORES_TABLE_SQL, OPENING_HOURS_TABLE_SQL, CREATE_INDEX_SQL


logger = logging.getLogger(__name__)


class DatabaseManager:
    """Manages SQLite database operations for store data."""

    def __init__(self, db_path: str = "stores.db"):
        """Initialize database connection and create tables if needed.

        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = db_path
        self.conn: Optional[sqlite3.Connection] = None
        self._connect()
        self._create_tables()

    def _connect(self):
        """Establish database connection."""
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.conn.row_factory = sqlite3.Row
            # Enable foreign keys
            self.conn.execute("PRAGMA foreign_keys = ON")
            logger.info(f"Connected to database: {self.db_path}")
        except sqlite3.Error as e:
            logger.error(f"Database connection error: {e}")
            raise

    def _create_tables(self):
        """Create database tables if they don't exist."""
        try:
            cursor = self.conn.cursor()
            cursor.execute(STORES_TABLE_SQL)
            cursor.execute(OPENING_HOURS_TABLE_SQL)
            cursor.execute(CREATE_INDEX_SQL)
            self.conn.commit()
            logger.info("Database tables created/verified")
        except sqlite3.Error as e:
            logger.error(f"Error creating tables: {e}")
            raise

    def upsert_store(self, store_data: Dict[str, Any]) -> int:
        """Insert or update a store record.

        Args:
            store_data: Dictionary containing store information

        Returns:
            market_id of the inserted/updated store
        """
        try:
            cursor = self.conn.cursor()

            # Check if store exists
            existing = cursor.execute(
                "SELECT market_id FROM stores WHERE market_id = ?",
                (store_data['market_id'],)
            ).fetchone()

            if existing:
                # Update existing store
                cursor.execute("""
                    UPDATE stores SET
                        name = ?, street = ?, zip = ?, city = ?,
                        latitude = ?, longitude = ?, phone = ?, email = ?,
                        country_code = ?, status = ?, opening_day = ?,
                        is_loyalty_market = ?, google_maps_link = ?,
                        updated_at = ?
                    WHERE market_id = ?
                """, (
                    store_data['name'], store_data.get('street'),
                    store_data.get('zip'), store_data.get('city'),
                    store_data.get('latitude'), store_data.get('longitude'),
                    store_data.get('phone'), store_data.get('email'),
                    store_data.get('country_code'), store_data.get('status'),
                    store_data.get('opening_day'), store_data.get('is_loyalty_market'),
                    store_data.get('google_maps_link'), datetime.now(),
                    store_data['market_id']
                ))
                logger.debug(f"Updated store: {store_data['market_id']}")
            else:
                # Insert new store
                cursor.execute("""
                    INSERT INTO stores (
                        market_id, name, street, zip, city,
                        latitude, longitude, phone, email,
                        country_code, status, opening_day,
                        is_loyalty_market, google_maps_link
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    store_data['market_id'], store_data['name'],
                    store_data.get('street'), store_data.get('zip'),
                    store_data.get('city'), store_data.get('latitude'),
                    store_data.get('longitude'), store_data.get('phone'),
                    store_data.get('email'), store_data.get('country_code'),
                    store_data.get('status'), store_data.get('opening_day'),
                    store_data.get('is_loyalty_market'),
                    store_data.get('google_maps_link')
                ))
                logger.debug(f"Inserted new store: {store_data['market_id']}")

            self.conn.commit()
            return store_data['market_id']

        except sqlite3.Error as e:
            logger.error(f"Error upserting store {store_data.get('market_id')}: {e}")
            self.conn.rollback()
            raise

    def insert_opening_hours(self, market_id: int, hours: List[Dict[str, Any]]):
        """Insert opening hours for a store.

        Args:
            market_id: Store's market ID
            hours: List of opening hours dictionaries
        """
        try:
            cursor = self.conn.cursor()

            # Delete existing hours for this store
            cursor.execute("DELETE FROM opening_hours WHERE market_id = ?", (market_id,))

            # Insert new hours
            for hour in hours:
                cursor.execute("""
                    INSERT INTO opening_hours (
                        market_id, weekday, open_from, open_until
                    ) VALUES (?, ?, ?, ?)
                """, (
                    market_id, hour.get('weekday'),
                    hour.get('open_from'), hour.get('open_until')
                ))

            self.conn.commit()
            logger.debug(f"Inserted {len(hours)} opening hours for store {market_id}")

        except sqlite3.Error as e:
            logger.error(f"Error inserting opening hours for {market_id}: {e}")
            self.conn.rollback()
            raise

    def get_store_count(self) -> int:
        """Get total number of stores in database.

        Returns:
            Count of stores
        """
        cursor = self.conn.cursor()
        count = cursor.execute("SELECT COUNT(*) FROM stores").fetchone()[0]
        return count

    def get_all_stores(self) -> List[Dict[str, Any]]:
        """Retrieve all stores from database.

        Returns:
            List of store dictionaries
        """
        cursor = self.conn.cursor()
        rows = cursor.execute("SELECT * FROM stores ORDER BY market_id").fetchall()
        return [dict(row) for row in rows]

    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
