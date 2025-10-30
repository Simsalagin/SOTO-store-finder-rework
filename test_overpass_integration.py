"""
Integration test for Overpass API client with real store data.

This script tests the OverpassClient with actual stores from the database
and real Overpass API calls to validate the implementation works in practice.
"""

import sqlite3
from src.services.overpass_client import OverpassClient


def get_test_stores(limit=10):
    """Get stores with valid coordinates for testing."""
    conn = sqlite3.connect('stores.db')
    cursor = conn.cursor()

    # Get stores with valid coordinates (not 0,0)
    cursor.execute("""
        SELECT market_id, name, street, zip, city, latitude, longitude
        FROM stores
        WHERE latitude != 0 AND longitude != 0
        LIMIT ?
    """, (limit,))

    stores = cursor.fetchall()
    conn.close()
    return stores


def test_overpass_with_real_data():
    """Test Overpass client with 10 real stores."""
    print("=" * 80)
    print("OVERPASS API INTEGRATION TEST - 10 REAL STORES")
    print("=" * 80)
    print()

    stores = get_test_stores(10)
    client = OverpassClient()

    results = {
        'exact_match': 0,
        'fuzzy_match': 0,
        'no_match': 0,
        'details': []
    }

    for i, store in enumerate(stores, 1):
        market_id, name, street, zip_code, city, lat, lon = store

        print(f"\n[{i}/10] Testing: {name}")
        print(f"   Address: {street}, {zip_code} {city}")
        print(f"   Scraper coords: {lat}, {lon}")

        # Try exact match first
        print(f"   → Trying exact match: '{name}'...")
        result = client.search_poi_exact(name, lat, lon, radius=100)

        if result:
            print(f"   ✅ EXACT MATCH FOUND!")
            print(f"      POI Name: {result['name']}")
            print(f"      POI Coords: {result['latitude']}, {result['longitude']}")
            print(f"      OSM ID: {result['osm_id']}")
            results['exact_match'] += 1
            results['details'].append({
                'store': name,
                'match_type': 'exact',
                'poi_name': result['name'],
                'poi_coords': (result['latitude'], result['longitude'])
            })
        else:
            # Try fuzzy match
            print(f"   ⚠️  No exact match")
            print(f"   → Trying fuzzy match: 'Denns'...")
            result = client.search_poi_fuzzy("Denns", lat, lon, radius=100)

            if result:
                print(f"   ✅ FUZZY MATCH FOUND!")
                print(f"      POI Name: {result['name']}")
                print(f"      POI Coords: {result['latitude']}, {result['longitude']}")
                print(f"      OSM ID: {result['osm_id']}")
                results['fuzzy_match'] += 1
                results['details'].append({
                    'store': name,
                    'match_type': 'fuzzy',
                    'poi_name': result['name'],
                    'poi_coords': (result['latitude'], result['longitude'])
                })
            else:
                print(f"   ❌ NO MATCH FOUND (will fall back to scraper coords)")
                results['no_match'] += 1
                results['details'].append({
                    'store': name,
                    'match_type': 'none',
                    'poi_name': None,
                    'poi_coords': None
                })

        # Rate limiting is now handled automatically by OverpassClient

    # Summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"Exact matches:    {results['exact_match']}/10 ({results['exact_match']*10}%)")
    print(f"Fuzzy matches:    {results['fuzzy_match']}/10 ({results['fuzzy_match']*10}%)")
    print(f"No matches:       {results['no_match']}/10 ({results['no_match']*10}%)")
    print(f"Total POIs found: {results['exact_match'] + results['fuzzy_match']}/10")
    print(f"\nTotal API calls:  {client.query_count}")
    print(f"Remaining today:  {client.daily_limit - client.query_count}/{client.daily_limit}")

    return results


if __name__ == '__main__':
    try:
        results = test_overpass_with_real_data()
    except KeyboardInterrupt:
        print("\n\nTest interrupted by user")
    except Exception as e:
        print(f"\n\n❌ Error during testing: {e}")
        import traceback
        traceback.print_exc()
