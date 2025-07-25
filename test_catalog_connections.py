#!/usr/bin/env python3
"""
Quick test to verify all catalog connections work with new naming
"""

import trino
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_catalog_connections():
    """Test connections to all three catalogs"""
    catalogs = {
        'iceberg_polaris': 'Polaris REST Catalog',
        'iceberg_hms': 'Hive Metastore Catalog', 
        'iceberg_nessie': 'Nessie Catalog'
    }
    
    results = {}
    
    for catalog_name, display_name in catalogs.items():
        try:
            logger.info(f"Testing {display_name} ({catalog_name})...")
            
            conn = trino.dbapi.connect(
                host='localhost',
                port=8081,
                user='admin',
                catalog=catalog_name,
                schema='default'
            )
            
            cursor = conn.cursor()
            cursor.execute("SHOW SCHEMAS")
            schemas = cursor.fetchall()
            
            conn.close()
            
            results[catalog_name] = True
            logger.info(f"✅ {display_name}: Connected successfully, found {len(schemas)} schemas")
            
        except Exception as e:
            results[catalog_name] = False
            logger.error(f"❌ {display_name}: Connection failed - {e}")
    
    return results

def main():
    print("CATALOG CONNECTION TEST")
    print("=" * 50)
    
    results = test_catalog_connections()
    
    print("\n" + "=" * 50)
    print("SUMMARY:")
    
    all_passed = True
    for catalog, passed in results.items():
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{catalog}: {status}")
        if not passed:
            all_passed = False
    
    if all_passed:
        print("\n🎉 All catalog connections successful!")
        print("Ready to run stress tests!")
    else:
        print("\n⚠️  Some catalog connections failed.")
        print("Please check Docker services and configuration.")

if __name__ == "__main__":
    main()
