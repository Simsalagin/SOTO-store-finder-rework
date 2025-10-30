"""Base scraper abstract class for all scrapers."""

from abc import ABC, abstractmethod
from typing import List, Dict, Any
import logging


logger = logging.getLogger(__name__)


class BaseScraper(ABC):
    """Abstract base class for all store scrapers."""

    def __init__(self, name: str):
        """Initialize scraper.

        Args:
            name: Name of the scraper
        """
        self.name = name
        self.logger = logging.getLogger(f"{__name__}.{name}")

    @abstractmethod
    def fetch_data(self) -> Any:
        """Fetch raw data from source.

        Returns:
            Raw data (format depends on source)

        Raises:
            Exception: If data fetching fails
        """
        pass

    @abstractmethod
    def parse_data(self, raw_data: Any) -> List[Dict[str, Any]]:
        """Parse raw data into standardized store format.

        Args:
            raw_data: Raw data from fetch_data()

        Returns:
            List of store dictionaries with standardized fields

        Raises:
            Exception: If parsing fails
        """
        pass

    def scrape(self) -> List[Dict[str, Any]]:
        """Execute complete scraping workflow.

        Returns:
            List of parsed store dictionaries

        Raises:
            Exception: If scraping fails
        """
        self.logger.info(f"Starting {self.name} scraper...")

        # Fetch raw data
        self.logger.info("Fetching data...")
        raw_data = self.fetch_data()

        # Parse data
        self.logger.info("Parsing data...")
        stores = self.parse_data(raw_data)

        self.logger.info(f"Successfully scraped {len(stores)} stores")
        return stores
