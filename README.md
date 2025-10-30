# Store Finder Scraper System

A modular, extensible Python-based scraper system for collecting and storing retail store location data from various sources. Built with clean code principles and designed for easy extension to support multiple data sources.

## Overview

This system scrapes store information (locations, contact details, opening hours) from retail websites and stores them in a normalized SQLite database with automatic duplicate prevention. The architecture is designed to make adding new scrapers straightforward while maintaining code quality and reliability.

## Features

- **Modular Architecture**: Easy addition of new scrapers through abstract base class
- **Duplicate Prevention**: Automatic detection using unique market IDs as primary keys
- **Data Validation**: Comprehensive validation of store data before insertion
- **Opening Hours Support**: Structured storage of store operating hours
- **Full Test Coverage**: Unit and integration tests for all components
- **SQLite Database**: Portable, serverless database with proper indexing
- **Clean Code**: Following SOLID principles and Python best practices
- **CLI Interface**: Simple command-line interface for running scrapers

## Current Scrapers

### Biomarkt (Denns BioMarkt)
- **Source**: https://www.biomarkt.de
- **Stores**: ~594 stores across Germany
- **Data**: Store locations, contact info, opening hours, coordinates

## Installation

### Prerequisites
- Python 3.10 or higher
- pip

### Setup

```bash
# Clone the repository
git clone <repository-url>
cd SOTO-store-finder-rework

# Install dependencies
pip install -r requirements.txt
```

## Usage

### Basic Usage

```bash
# Run the Biomarkt scraper
python src/main.py --scraper biomarkt

# Run with verbose logging
python src/main.py --scraper biomarkt --verbose

# Specify custom database path
python src/main.py --scraper biomarkt --db custom_path.db
```

### Expected Output

```
2025-10-30 13:18:31 - Running biomarkt scraper...
2025-10-30 13:18:32 - Successfully scraped 594 stores
2025-10-30 13:18:33 - ✓ Successfully saved 594 stores
2025-10-30 13:18:33 -   - New stores: 594
2025-10-30 13:18:33 -   - Updated stores: 0
2025-10-30 13:18:33 -   - Total stores in database: 594
```

## Database Schema

### Tables

#### `stores`
Main store information table with duplicate prevention via `market_id` primary key.

| Column | Type | Description |
|--------|------|-------------|
| market_id | INTEGER (PK) | Unique store identifier |
| name | TEXT | Store name |
| street | TEXT | Street address |
| zip | TEXT | Postal code |
| city | TEXT | City name |
| latitude | REAL | GPS latitude |
| longitude | REAL | GPS longitude |
| phone | TEXT | Contact phone |
| email | TEXT | Contact email |
| country_code | TEXT | ISO country code |
| status | TEXT | Store status |
| opening_day | DATE | Store opening date |
| is_loyalty_market | BOOLEAN | Loyalty program flag |
| google_maps_link | TEXT | Google Maps URL |
| created_at | TIMESTAMP | Record creation time |
| updated_at | TIMESTAMP | Last update time |

#### `opening_hours`
Store opening hours with foreign key relationship to stores.

| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER (PK) | Auto-increment ID |
| market_id | INTEGER (FK) | Reference to stores.market_id |
| weekday | TEXT | Day of week |
| open_from | TEXT | Opening time (HH:MM) |
| open_until | TEXT | Closing time (HH:MM) |

## Testing

### Run All Tests

```bash
# All tests
pytest tests/ -v

# Unit tests only (no network calls)
pytest tests/ -v -m "not integration"

# Integration tests only (requires network)
pytest tests/ -v -m "integration"

# With coverage report
pytest tests/ --cov=src --cov-report=html
```

### Test Structure

```
tests/
├── conftest.py                  # Pytest configuration
├── test_biomarkt_scraper.py    # Biomarkt scraper tests
└── test_db_manager.py          # Database manager tests
```

## Project Structure

```
.
├── src/
│   ├── __init__.py
│   ├── main.py                 # CLI entry point
│   ├── scrapers/
│   │   ├── __init__.py
│   │   ├── base_scraper.py     # Abstract base class
│   │   └── biomarkt_scraper.py # Biomarkt implementation
│   ├── database/
│   │   ├── __init__.py
│   │   ├── models.py           # Database schema
│   │   └── db_manager.py       # Database operations
│   └── utils/
│       ├── __init__.py
│       └── validators.py       # Data validation
├── tests/
│   ├── __init__.py
│   ├── conftest.py
│   ├── test_biomarkt_scraper.py
│   └── test_db_manager.py
├── requirements.txt
├── pytest.ini
├── .gitignore
└── README.md
```

## Adding New Scrapers

See [CONTRIBUTING.md](CONTRIBUTING.md) for detailed instructions on adding new scrapers.

### Quick Start

1. **Create Scraper Class**
   ```python
   # src/scrapers/newscraper.py
   from .base_scraper import BaseScraper

   class NewScraper(BaseScraper):
       def __init__(self):
           super().__init__("newscraper")

       def fetch_data(self):
           # Implement data fetching
           pass

       def parse_data(self, raw_data):
           # Implement data parsing
           return stores_list
   ```

2. **Register Scraper**
   ```python
   # src/main.py
   from scrapers.newscraper import NewScraper

   SCRAPERS = {
       'biomarkt': BiomarktScraper,
       'newscraper': NewScraper,  # Add here
   }
   ```

3. **Add Tests**
   ```python
   # tests/test_newscraper.py
   def test_newscraper():
       scraper = NewScraper()
       # Add test cases
   ```

## Development

### Code Style
- Follow PEP 8 guidelines
- Use type hints where applicable
- Write comprehensive docstrings
- Keep functions small and focused

### Logging
The system uses Python's built-in logging module. Set verbosity with `--verbose` flag.

## Troubleshooting

### Common Issues

**Issue**: Tests fail with import errors
```bash
# Solution: Ensure you're in the project root directory
cd /path/to/SOTO-store-finder-rework
pytest tests/
```

**Issue**: Database locked error
```bash
# Solution: Close any open database connections
rm stores.db  # If safe to delete
python src/main.py --scraper biomarkt
```

## License

[Add your license here]

## Contributing

Contributions are welcome! Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on our code of conduct and the process for submitting pull requests.

## Authors

- Initial implementation by Claude Code

## Acknowledgments

- Built following lean development and clean code principles
- Designed for extensibility and maintainability
