"""
Integration test for enhanced Overpass API client with real store data.

This script tests the complete 3-tier geocoding strategy:
- Tier 1: Overpass POI search with name variants and validation
- Tier 2: Scraper coordinates
- Tier 3: Nominatim address-level fallback

Tests with real Overpass API calls and shows confidence levels and
address correction scenarios.
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
    """Test enhanced Overpass client with 10 real stores using 3-tier strategy."""
    print("=" * 80)
    print("ENHANCED OVERPASS API INTEGRATION TEST - 3-TIER GEOCODING")
    print("=" * 80)
    print()
    print("Testing with name variants, validation, and confidence levels")
    print()

    stores = get_test_stores(10)
    client = OverpassClient()

    results = {
        'very_high': 0,  # POI + validated by scraper
        'high': 0,       # POI found (no validation or conflict)
        'medium': 0,     # No POI, using scraper
        'details': []
    }

    for i, store in enumerate(stores, 1):
        market_id, name, street, zip_code, city, lat, lon = store

        print(f"\n{'='*80}")
        print(f"[{i}/10] {name}")
        print(f"{'='*80}")
        print(f"   Scraper Address: {street}, {zip_code} {city}")
        print(f"   Scraper Coords:  {lat:.6f}, {lon:.6f}")
        print()

        # Use search_poi_with_variants (tries 5 variants automatically)
        print(f"   → Searching with name variants...")
        result = client.search_poi_with_variants(name, lat, lon, radius=100)

        if result:
            print(f"   ✅ POI FOUND!")
            print(f"      POI Name:    {result['name']}")
            print(f"      Matched via: {result.get('matched_variant', 'unknown')}")
            print(f"      POI Coords:  {result['latitude']:.6f}, {result['longitude']:.6f}")
            print(f"      OSM ID:      {result['osm_id']}")

            # Show OSM address if available
            if result.get('street'):
                osm_addr = f"{result.get('housenumber', '')} {result['street']}".strip()
                osm_addr += f", {result.get('postcode', '')} {result.get('city', '')}".strip()
                print(f"      OSM Address: {osm_addr}")

            # Validate against scraper
            distance, is_valid = client.validate_poi_against_scraper(
                result['latitude'], result['longitude'], lat, lon, threshold=100.0
            )

            print()
            print(f"   📏 Distance Validation:")
            print(f"      Distance from scraper: {distance:.1f}m")
            print(f"      Threshold: 100m")

            if is_valid:
                print(f"      ✅ VALIDATED - Distance < 100m")
                print(f"      → Confidence: VERY_HIGH")
                print(f"      → Action: Use POI coords, keep scraper address")
                results['very_high'] += 1
                confidence = 'very_high'
            else:
                print(f"      ⚠️  CONFLICT - Distance > 100m!")
                print(f"      → Confidence: HIGH")
                print(f"      → Action: Use POI coords, UPDATE address from OSM")
                print(f"      → Old address will be replaced with OSM data")
                results['high'] += 1
                confidence = 'high'

            results['details'].append({
                'store': name,
                'confidence': confidence,
                'poi_name': result['name'],
                'distance': distance,
                'osm_address': result.get('street')
            })
        else:
            print(f"   ❌ NO POI FOUND")
            print(f"      → Tier 2: Using scraper coordinates")
            print(f"      → Confidence: MEDIUM")
            results['medium'] += 1
            results['details'].append({
                'store': name,
                'confidence': 'medium',
                'poi_name': None,
                'distance': None,
                'osm_address': None
            })

    # Summary
    print("\n" + "=" * 80)
    print("SUMMARY - CONFIDENCE DISTRIBUTION")
    print("=" * 80)
    print()
    print(f"📊 Confidence Levels:")
    print(f"   VERY_HIGH (POI + validated):  {results['very_high']}/10 ({results['very_high']*10}%)")
    print(f"   HIGH (POI, no validation):    {results['high']}/10 ({results['high']*10}%)")
    print(f"   MEDIUM (Scraper only):        {results['medium']}/10 ({results['medium']*10}%)")
    print()
    total_poi = results['very_high'] + results['high']
    print(f"🎯 Total POIs found: {total_poi}/10 ({total_poi*10}%)")
    print()
    print(f"🔧 API Statistics:")
    print(f"   Total API calls:  {client.query_count}")
    print(f"   Remaining today:  {client.daily_limit - client.query_count}/{client.daily_limit}")
    print()

    # Show stores that would have address corrected
    conflicts = [d for d in results['details'] if d['confidence'] == 'high']
    if conflicts:
        print(f"⚠️  Address Corrections ({len(conflicts)} stores):")
        for detail in conflicts:
            print(f"   - {detail['store']}: {detail['distance']:.0f}m mismatch")
            if detail['osm_address']:
                print(f"     Will update to: {detail['osm_address']}")
        print()

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
