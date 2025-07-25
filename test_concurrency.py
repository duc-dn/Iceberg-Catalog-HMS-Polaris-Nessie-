#!/usr/bin/env python3
"""
Concurrency test script for Polaris and Hive Metastore with Trino
Tests concurrent operations on different Iceberg catalogs:
- polaris: Uses Polaris catalog
- iceberg_hms: Uses Hive Metastore
"""

import threading
import time
import random
import logging
from typing import List, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
import trino

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(threadName)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TrinoConcurrencyTester:
    def __init__(self, host='localhost', port=8081, user='admin'):
        self.host = host
        self.port = port
        self.user = user
        self.polaris_conn = None
        self.hms_conn = None
        self.setup_connections()
        
    def setup_connections(self):
        """Setup connections to both catalogs"""
        try:
            # Connection for Polaris catalog
            self.polaris_conn = trino.dbapi.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                catalog='iceberg_polaris',
                schema='default'
            )
            logger.info("Connected to Polaris catalog")
            
            # Connection for HMS catalog
            self.hms_conn = trino.dbapi.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                catalog='iceberg_hms',
                schema='default'
            )
            logger.info("Connected to HMS catalog")
            
        except Exception as e:
            logger.error(f"Failed to setup connections: {e}")
            raise

    def execute_query(self, connection, query: str, catalog_name: str) -> Dict[str, Any]:
        """Execute a query and return results with timing"""
        start_time = time.time()
        thread_id = threading.current_thread().name
        
        try:
            cursor = connection.cursor()
            logger.info(f"[{catalog_name}] Executing: {query[:100]}...")
            
            cursor.execute(query)
            
            # Fetch results if it's a SELECT query
            if query.strip().upper().startswith('SELECT'):
                results = cursor.fetchall()
                rows_count = len(results)
            else:
                results = None
                rows_count = cursor.rowcount if hasattr(cursor, 'rowcount') else 0
            
            end_time = time.time()
            duration = end_time - start_time
            
            logger.info(f"[{catalog_name}] Query completed in {duration:.2f}s, rows: {rows_count}")
            
            return {
                'catalog': catalog_name,
                'query': query,
                'duration': duration,
                'rows_count': rows_count,
                'success': True,
                'thread_id': thread_id,
                'results': results
            }
            
        except Exception as e:
            end_time = time.time()
            duration = end_time - start_time
            logger.error(f"[{catalog_name}] Query failed after {duration:.2f}s: {e}")
            
            return {
                'catalog': catalog_name,
                'query': query,
                'duration': duration,
                'success': False,
                'error': str(e),
                'thread_id': thread_id
            }

    def setup_test_environment(self):
        """Setup schemas and tables for testing"""
        setup_queries = [
            # Polaris setup
            ("iceberg", "CREATE SCHEMA IF NOT EXISTS test_schema"),
            ("iceberg", "DROP TABLE IF EXISTS test_schema.concurrent_test"),
            ("iceberg", """
                CREATE TABLE test_schema.concurrent_test (
                    id bigint,
                    name varchar,
                    value double,
                    created_at timestamp,
                    partition_date date
                ) WITH (
                    partitioning = ARRAY['partition_date']
                )
            """),
            
            # HMS setup
            ("iceberg_hms", "CREATE SCHEMA IF NOT EXISTS test_schema"),
            ("iceberg_hms", "DROP TABLE IF EXISTS test_schema.concurrent_test"),
            ("iceberg_hms", """
                CREATE TABLE test_schema.concurrent_test (
                    id bigint,
                    name varchar,
                    value double,
                    created_at timestamp,
                    partition_date date
                ) WITH (
                    partitioning = ARRAY['partition_date']
                )
            """)
        ]
        
        for catalog_name, query in setup_queries:
            conn = self.polaris_conn if catalog_name == "iceberg" else self.hms_conn
            result = self.execute_query(conn, query, catalog_name)
            if not result['success']:
                logger.error(f"Setup failed for {catalog_name}: {result.get('error')}")

    def generate_test_data_query(self, table_name: str, batch_size: int = 100) -> str:
        """Generate INSERT query with test data"""
        values = []
        current_date = time.strftime('%Y-%m-%d')
        
        for i in range(batch_size):
            id_val = random.randint(1, 1000000)
            name_val = f"'name_{id_val}'"
            value_val = round(random.uniform(1.0, 1000.0), 2)
            created_at = f"TIMESTAMP '{current_date} {random.randint(0,23):02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}'"
            partition_date = f"DATE '{current_date}'"
            
            values.append(f"({id_val}, {name_val}, {value_val}, {created_at}, {partition_date})")
        
        return f"INSERT INTO {table_name} VALUES {', '.join(values)}"

    def concurrent_insert_test(self, num_threads: int = 5, inserts_per_thread: int = 3):
        """Test concurrent inserts to both catalogs"""
        logger.info(f"Starting concurrent insert test with {num_threads} threads, {inserts_per_thread} inserts per thread")
        
        def insert_worker(catalog_name: str, thread_num: int):
            conn = self.polaris_conn if catalog_name == "iceberg" else self.hms_conn
            results = []
            
            for i in range(inserts_per_thread):
                query = self.generate_test_data_query("test_schema.concurrent_test", 50)
                result = self.execute_query(conn, query, f"{catalog_name}-T{thread_num}")
                results.append(result)
                
                # Small delay between inserts
                time.sleep(random.uniform(0.1, 0.5))
            
            return results
        
        # Create tasks for both catalogs
        tasks = []
        with ThreadPoolExecutor(max_workers=num_threads * 2) as executor:
            # Submit tasks for Polaris
            for i in range(num_threads):
                task = executor.submit(insert_worker, "iceberg", i)
                tasks.append(task)
            
            # Submit tasks for HMS
            for i in range(num_threads):
                task = executor.submit(insert_worker, "iceberg_hms", i)
                tasks.append(task)
            
            # Collect results
            all_results = []
            for task in as_completed(tasks):
                try:
                    results = task.result()
                    all_results.extend(results)
                except Exception as e:
                    logger.error(f"Task failed: {e}")
        
        return all_results

    def concurrent_read_test(self, num_threads: int = 5, queries_per_thread: int = 3):
        """Test concurrent reads from both catalogs"""
        logger.info(f"Starting concurrent read test with {num_threads} threads, {queries_per_thread} queries per thread")
        
        read_queries = [
            "SELECT COUNT(*) FROM test_schema.concurrent_test",
            "SELECT partition_date, COUNT(*) FROM test_schema.concurrent_test GROUP BY 1",
            "SELECT partition_date, AVG(value) FROM test_schema.concurrent_test GROUP BY partition_date",
            "SELECT * FROM test_schema.concurrent_test ORDER BY id DESC LIMIT 10",
            "SELECT name, MAX(value), MIN(value) FROM test_schema.concurrent_test GROUP BY name LIMIT 5"
        ]
        
        def read_worker(catalog_name: str, thread_num: int):
            conn = self.polaris_conn if catalog_name == "iceberg" else self.hms_conn
            results = []
            
            for i in range(queries_per_thread):
                query = random.choice(read_queries)
                result = self.execute_query(conn, query, f"{catalog_name}-R{thread_num}")
                results.append(result)
                
                # Small delay between queries
                time.sleep(random.uniform(0.1, 0.3))
            
            return results
        
        # Create tasks for both catalogs
        tasks = []
        with ThreadPoolExecutor(max_workers=num_threads * 2) as executor:
            # Submit tasks for Polaris
            for i in range(num_threads):
                task = executor.submit(read_worker, "iceberg", i)
                tasks.append(task)
            
            # Submit tasks for HMS
            for i in range(num_threads):
                task = executor.submit(read_worker, "iceberg_hms", i)
                tasks.append(task)
            
            # Collect results
            all_results = []
            for task in as_completed(tasks):
                try:
                    results = task.result()
                    all_results.extend(results)
                except Exception as e:
                    logger.error(f"Task failed: {e}")
        
        return all_results

    def mixed_workload_test(self, num_threads: int = 4, operations_per_thread: int = 5):
        """Test mixed read/write workload"""
        logger.info(f"Starting mixed workload test with {num_threads} threads, {operations_per_thread} operations per thread")
        
        def mixed_worker(catalog_name: str, thread_num: int):
            conn = self.polaris_conn if catalog_name == "iceberg" else self.hms_conn
            results = []
            
            for i in range(operations_per_thread):
                # Randomly choose between read and write operations
                if random.random() < 0.6:  # 60% reads, 40% writes
                    # Read operation
                    query = "SELECT COUNT(*) FROM test_schema.concurrent_test WHERE value > " + str(random.randint(100, 500))
                else:
                    # Write operation
                    query = self.generate_test_data_query("test_schema.concurrent_test", 25)
                
                result = self.execute_query(conn, query, f"{catalog_name}-M{thread_num}")
                results.append(result)
                
                # Random delay
                time.sleep(random.uniform(0.1, 0.8))
            
            return results
        
        # Create tasks for both catalogs
        tasks = []
        with ThreadPoolExecutor(max_workers=num_threads * 2) as executor:
            # Submit tasks for both catalogs
            for catalog in ["iceberg", "iceberg_hms"]:
                for i in range(num_threads):
                    task = executor.submit(mixed_worker, catalog, i)
                    tasks.append(task)
            
            # Collect results
            all_results = []
            for task in as_completed(tasks):
                try:
                    results = task.result()
                    all_results.extend(results)
                except Exception as e:
                    logger.error(f"Task failed: {e}")
        
        return all_results

    def analyze_results(self, results: List[Dict[str, Any]]):
        """Analyze and print test results"""
        polaris_results = [r for r in results if 'iceberg' in r['catalog']]
        hms_results = [r for r in results if 'iceberg_hms' in r['catalog']]
        
        successful_polaris = [r for r in polaris_results if r['success']]
        successful_hms = [r for r in hms_results if r['success']]
        
        failed_polaris = [r for r in polaris_results if not r['success']]
        failed_hms = [r for r in hms_results if not r['success']]
        
        print("\n" + "="*80)
        print("CONCURRENCY TEST RESULTS")
        print("="*80)
        
        print(f"\nPOLARIS CATALOG:")
        print(f"  Total operations: {len(polaris_results)}")
        print(f"  Successful: {len(successful_polaris)}")
        print(f"  Failed: {len(failed_polaris)}")
        if successful_polaris:
            avg_duration = sum(r['duration'] for r in successful_polaris) / len(successful_polaris)
            print(f"  Average duration: {avg_duration:.2f}s")
            print(f"  Min duration: {min(r['duration'] for r in successful_polaris):.2f}s")
            print(f"  Max duration: {max(r['duration'] for r in successful_polaris):.2f}s")
        
        print(f"\nHIVE METASTORE CATALOG:")
        print(f"  Total operations: {len(hms_results)}")
        print(f"  Successful: {len(successful_hms)}")
        print(f"  Failed: {len(failed_hms)}")
        if successful_hms:
            avg_duration = sum(r['duration'] for r in successful_hms) / len(successful_hms)
            print(f"  Average duration: {avg_duration:.2f}s")
            print(f"  Min duration: {min(r['duration'] for r in successful_hms):.2f}s")
            print(f"  Max duration: {max(r['duration'] for r in successful_hms):.2f}s")
        
        # Print failed operations
        if failed_polaris:
            print(f"\nFAILED POLARIS OPERATIONS:")
            for r in failed_polaris:
                print(f"  - {r['query'][:60]}... : {r['error']}")
        
        if failed_hms:
            print(f"\nFAILED HMS OPERATIONS:")
            for r in failed_hms:
                print(f"  - {r['query'][:60]}... : {r['error']}")
        
        print("\n" + "="*80)

    def run_all_tests(self):
        """Run all concurrency tests"""
        logger.info("Starting comprehensive concurrency test suite")
        
        # Setup test environment
        print("Setting up test environment...")
        self.setup_test_environment()
        
        all_results = []
        
        # Test 1: Concurrent Inserts
        print("\n1. Running concurrent insert test...")
        insert_results = self.concurrent_insert_test(num_threads=3, inserts_per_thread=2)
        all_results.extend(insert_results)
        
        # Wait a bit between tests
        time.sleep(2)
        
        # Test 2: Concurrent Reads
        print("\n2. Running concurrent read test...")
        read_results = self.concurrent_read_test(num_threads=4, queries_per_thread=3)
        all_results.extend(read_results)
        
        # Wait a bit between tests
        time.sleep(2)
        
        # Test 3: Mixed Workload
        print("\n3. Running mixed workload test...")
        mixed_results = self.mixed_workload_test(num_threads=3, operations_per_thread=4)
        all_results.extend(mixed_results)
        
        # Analyze all results
        self.analyze_results(all_results)
        
        return all_results

    def cleanup(self):
        """Clean up connections"""
        if self.polaris_conn:
            self.polaris_conn.close()
        if self.hms_conn:
            self.hms_conn.close()

def main():
    """Main function to run the concurrency tests"""
    tester = None
    try:
        print("Initializing Trino Concurrency Tester...")
        tester = TrinoConcurrencyTester()
        
        # Run all tests
        results = tester.run_all_tests()
        
        print(f"\nTest completed! Total operations: {len(results)}")
        
    except Exception as e:
        logger.error(f"Test failed: {e}")
        raise
    finally:
        if tester:
            tester.cleanup()

if __name__ == "__main__":
    main()
