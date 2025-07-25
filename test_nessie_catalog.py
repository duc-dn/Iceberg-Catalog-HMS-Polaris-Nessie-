#!/usr/bin/env python3
"""
Test script to verify Nessie catalog connectivity and setup
"""

import time
import trino
import requests
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def wait_for_nessie(max_retries=30, delay=5):
    """Wait for Nessie service to be ready"""
    nessie_url = "http://localhost:19120/api/v2/config"
    
    for attempt in range(max_retries):
        try:
            response = requests.get(nessie_url, timeout=5)
            if response.status_code == 200:
                logger.info("Nessie service is ready!")
                return True
        except requests.exceptions.RequestException as e:
            logger.info(f"Attempt {attempt + 1}: Waiting for Nessie... ({e})")
            time.sleep(delay)
    
    logger.error("Nessie service did not become ready in time")
    return False

def test_nessie_catalog():
    """Test Nessie catalog connectivity through Trino"""
    try:
        # Connect to Trino
        conn = trino.dbapi.connect(
            host='localhost',
            port=8081,
            user='admin',
            catalog='iceberg_nessie',
            schema='default'
        )
        
        cursor = conn.cursor()
        
        # Test basic connectivity
        logger.info("Testing Nessie catalog connectivity...")
        cursor.execute("SHOW SCHEMAS")
        schemas = cursor.fetchall()
        logger.info(f"Available schemas in Nessie catalog: {schemas}")
        
        # Create a test schema
        logger.info("Creating test schema...")
        cursor.execute("CREATE SCHEMA IF NOT EXISTS test_nessie")
        
        # Create a test table
        logger.info("Creating test table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS test_nessie.sample_table (
                id bigint,
                name varchar,
                created_at timestamp,
                partition_date date
            ) WITH (
                partitioning = ARRAY['partition_date']
            )
        """)
        
        # Insert test data
        logger.info("Inserting test data...")
        cursor.execute("""
            INSERT INTO test_nessie.sample_table VALUES 
            (1, 'test_nessie_1', current_timestamp, current_date),
            (2, 'test_nessie_2', current_timestamp, current_date)
        """)
        
        # Query test data
        logger.info("Querying test data...")
        cursor.execute("SELECT * FROM test_nessie.sample_table")
        results = cursor.fetchall()
        logger.info(f"Test data: {results}")
        
        # Show table info
        cursor.execute("SHOW CREATE TABLE test_nessie.sample_table")
        table_def = cursor.fetchall()
        logger.info(f"Table definition: {table_def}")
        
        logger.info("✅ Nessie catalog test completed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Nessie catalog test failed: {e}")
        return False
    finally:
        if 'conn' in locals():
            conn.close()

def test_nessie_api():
    """Test Nessie REST API directly"""
    try:
        logger.info("Testing Nessie REST API...")
        
        # Get default branch
        response = requests.get("http://localhost:19120/api/v2/trees/main")
        if response.status_code == 200:
            logger.info(f"Main branch info: {response.json()}")
        else:
            logger.warning(f"Could not get main branch info: {response.status_code}")
        
        # List namespaces
        response = requests.get("http://localhost:19120/api/v2/trees/main/contents")
        if response.status_code == 200:
            contents = response.json()
            logger.info(f"Nessie contents: {contents}")
        else:
            logger.warning(f"Could not list contents: {response.status_code}")
        
        logger.info("✅ Nessie API test completed!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Nessie API test failed: {e}")
        return False

def main():
    """Main function"""
    print("NESSIE CATALOG CONNECTIVITY TEST")
    print("=" * 50)
    
    # Wait for Nessie to be ready
    if not wait_for_nessie():
        print("❌ Nessie service is not ready. Please check the Docker containers.")
        return False
    
    # Test Nessie API
    api_success = test_nessie_api()
    
    # Test Nessie catalog through Trino
    catalog_success = test_nessie_catalog()
    
    if api_success and catalog_success:
        print("\n✅ All Nessie tests passed! The catalog is ready for use.")
        return True
    else:
        print("\n❌ Some Nessie tests failed. Please check the logs above.")
        return False

if __name__ == "__main__":
    main()
