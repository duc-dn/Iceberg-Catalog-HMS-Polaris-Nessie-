#!/usr/bin/env python3
"""
Quick test script for DELETE stress test functionality
"""

import time
import trino
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_delete_functionality():
    """Test basic DELETE functionality on all catalogs"""
    catalogs = {
        'Polaris': 'iceberg_polaris',
        'HMS': 'iceberg_hms', 
        'Nessie': 'iceberg_nessie'
    }
    
    print("TESTING DELETE FUNCTIONALITY ON ALL CATALOGS")
    print("=" * 50)
    
    for catalog_name, catalog_id in catalogs.items():
        print(f"\nTesting {catalog_name} catalog ({catalog_id})...")
        
        try:
            # Connect to catalog
            conn = trino.dbapi.connect(
                host='localhost',
                port=8081,
                user='admin',
                catalog=catalog_id,
                schema='default'
            )
            
            cursor = conn.cursor()
            table_name = f"test_delete_{int(time.time())}"
            
            # Create test table
            cursor.execute("CREATE SCHEMA IF NOT EXISTS test_delete")
            
            cursor.execute(f"""
                CREATE TABLE test_delete.{table_name} (
                    id bigint,
                    category varchar,
                    value double
                )
            """)
            
            # Insert test data
            cursor.execute(f"""
                INSERT INTO test_delete.{table_name} VALUES 
                (1, 'A', 10.0),
                (2, 'B', 20.0),
                (3, 'A', 30.0),
                (4, 'C', 40.0),
                (5, 'B', 50.0)
            """)
            
            # Test different delete patterns
            print(f"  - Testing delete by category...")
            cursor.execute(f"DELETE FROM test_delete.{table_name} WHERE category = 'A'")
            
            print(f"  - Testing delete by value range...")
            cursor.execute(f"DELETE FROM test_delete.{table_name} WHERE value > 25.0")
            
            # Check remaining data
            cursor.execute(f"SELECT COUNT(*) FROM test_delete.{table_name}")
            remaining_count = cursor.fetchone()[0]
            print(f"  - Remaining rows: {remaining_count}")
            
            # Cleanup
            cursor.execute(f"DROP TABLE test_delete.{table_name}")
            
            conn.close()
            print(f"  ✓ {catalog_name} catalog DELETE test PASSED")
            
        except Exception as e:
            print(f"  ✗ {catalog_name} catalog DELETE test FAILED: {e}")
    
    print(f"\nDELETE functionality test completed!")
    print("You can now run the full DELETE stress test:")
    print("  python stress_test_delete.py")

if __name__ == "__main__":
    test_delete_functionality()
