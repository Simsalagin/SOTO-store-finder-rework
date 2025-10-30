"""Main CLI entry point for store scraper system."""

import argparse
import logging
import sys
from typing import Optional

from scrapers.biomarkt_scraper import BiomarktScraper
from database.db_manager import DatabaseManager


# Available scrapers registry
SCRAPERS = {
    'biomarkt': BiomarktScraper,
}


def setup_logging(verbose: bool = False):
    """Configure logging.

    Args:
        verbose: Enable verbose logging
    """
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def run_scraper(scraper_name: str, db_path: str = "stores.db", verbose: bool = False) -> int:
    """Run specified scraper and store data.

    Args:
        scraper_name: Name of scraper to run
        db_path: Path to database file
        verbose: Enable verbose output

    Returns:
        Exit code (0 for success, 1 for error)
    """
    setup_logging(verbose)
    logger = logging.getLogger(__name__)

    # Validate scraper name
    if scraper_name not in SCRAPERS:
        logger.error(f"Unknown scraper: {scraper_name}")
        logger.info(f"Available scrapers: {', '.join(SCRAPERS.keys())}")
        return 1

    try:
        # Initialize scraper
        scraper_class = SCRAPERS[scraper_name]
        scraper = scraper_class()

        # Scrape data
        logger.info(f"Running {scraper_name} scraper...")
        stores = scraper.scrape()

        if not stores:
            logger.warning("No stores found!")
            return 1

        # Save to database
        logger.info(f"Saving {len(stores)} stores to database...")
        with DatabaseManager(db_path) as db:
            duplicates = 0
            new_stores = 0

            for store in stores:
                # Check if already exists
                existing_count = db.get_store_count()

                # Upsert store
                opening_hours = store.pop('opening_hours', [])
                db.upsert_store(store)

                # Insert opening hours
                if opening_hours:
                    db.insert_opening_hours(store['market_id'], opening_hours)

                # Track new vs updated
                new_count = db.get_store_count()
                if new_count > existing_count:
                    new_stores += 1
                else:
                    duplicates += 1

            total_count = db.get_store_count()

            logger.info(f"✓ Successfully saved {len(stores)} stores")
            logger.info(f"  - New stores: {new_stores}")
            logger.info(f"  - Updated stores: {duplicates}")
            logger.info(f"  - Total stores in database: {total_count}")

        return 0

    except Exception as e:
        logger.error(f"Scraping failed: {e}", exc_info=verbose)
        return 1


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Store scraper system - collect and store retail location data"
    )
    parser.add_argument(
        '--scraper',
        required=True,
        choices=SCRAPERS.keys(),
        help="Scraper to run"
    )
    parser.add_argument(
        '--db',
        default='stores.db',
        help="Database file path (default: stores.db)"
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help="Enable verbose output"
    )

    args = parser.parse_args()

    exit_code = run_scraper(args.scraper, args.db, args.verbose)
    sys.exit(exit_code)


if __name__ == '__main__':
    main()
