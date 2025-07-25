#!/usr/bin/env python3
"""
Stress test for INSERT operations on Polaris and Hive Metastore catalogs
This script performs intensive concurrent insert operations to test the limits
"""

import threading
import time
import random
import logging
import statistics
from typing import List, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
import trino

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(threadName)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class InsertStressTester:
    def __init__(self, host='localhost', port=8081, user='admin'):
        self.host = host
        self.port = port
        self.user = user
        self.polaris_conn = None
        self.hms_conn = None
        self.nessie_conn = None
        # Use timestamp to create unique table names
        self.table_suffix = str(int(time.time()))
        self.table_name = f"insert_stress_{self.table_suffix}"
        self.setup_connections()
        
    def setup_connections(self):
        """Setup connections to all catalogs"""
        try:
            # Connection for Polaris catalog (iceberg_polaris)
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
            
            # Connection for Nessie catalog
            self.nessie_conn = trino.dbapi.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                catalog='iceberg_nessie',
                schema='default'
            )
            logger.info("Connected to Nessie catalog")
            
        except Exception as e:
            logger.error(f"Failed to setup connections: {e}")
            raise

    def execute_query(self, connection, query: str, catalog_name: str) -> Dict[str, Any]:
        """Execute a query and return results with timing"""
        start_time = time.time()
        thread_id = threading.current_thread().name
        
        try:
            cursor = connection.cursor()
            
            cursor.execute(query)
            rows_count = cursor.rowcount if hasattr(cursor, 'rowcount') else 0
            
            end_time = time.time()
            duration = end_time - start_time
            
            return {
                'catalog': catalog_name,
                'duration': duration,
                'rows_count': rows_count,
                'success': True,
                'thread_id': thread_id,
                'timestamp': start_time
            }
            
        except Exception as e:
            end_time = time.time()
            duration = end_time - start_time
            
            return {
                'catalog': catalog_name,
                'duration': duration,
                'success': False,
                'error': str(e),
                'thread_id': thread_id,
                'timestamp': start_time
            }

    def setup_stress_environment(self):
        """Setup schemas and tables for stress testing"""
        setup_queries = [
            # Polaris setup
            ("iceberg_polaris", "CREATE SCHEMA IF NOT EXISTS stress_test"),
            
            # HMS setup
            ("iceberg_hms", "CREATE SCHEMA IF NOT EXISTS stress_test"),
            
            # Nessie setup
            ("iceberg_nessie", "CREATE SCHEMA IF NOT EXISTS stress_test"),
        ]
        
        # Try to drop existing tables, but don't fail if they don't exist
        drop_queries = [
            ("iceberg_polaris", f"DROP TABLE IF EXISTS stress_test.{self.table_name}"),
            ("iceberg_hms", f"DROP TABLE IF EXISTS stress_test.{self.table_name}"),
            ("iceberg_nessie", f"DROP TABLE IF EXISTS stress_test.{self.table_name}")
        ]
        
        create_queries = [
            # Polaris table creation
            ("iceberg_polaris", f"""
                CREATE TABLE stress_test.{self.table_name} (
                    id bigint,
                    batch_id bigint,
                    thread_id varchar,
                    name varchar,
                    value double,
                    random_data varchar,
                    created_at timestamp,
                    partition_date date
                ) WITH (
                    partitioning = ARRAY['partition_date']
                )
            """),
            
            # HMS table creation
            ("iceberg_hms", f"""
                CREATE TABLE stress_test.{self.table_name} (
                    id bigint,
                    batch_id bigint,
                    thread_id varchar,
                    name varchar,
                    value double,
                    random_data varchar,
                    created_at timestamp,
                    partition_date date
                ) WITH (
                    partitioning = ARRAY['partition_date']
                )
            """),
            
            # Nessie table creation
            ("iceberg_nessie", f"""
                CREATE TABLE stress_test.{self.table_name} (
                    id bigint,
                    batch_id bigint,
                    thread_id varchar,
                    name varchar,
                    value double,
                    random_data varchar,
                    created_at timestamp,
                    partition_date date
                ) WITH (
                    partitioning = ARRAY['partition_date']
                )
            """)
        ]
        
        # Execute schema creation
        for catalog_name, query in setup_queries:
            if catalog_name == "iceberg_polaris":
                conn = self.polaris_conn
            elif catalog_name == "iceberg_hms":
                conn = self.hms_conn
            else:  # iceberg_nessie
                conn = self.nessie_conn
            result = self.execute_query(conn, query, catalog_name)
            if not result['success']:
                logger.warning(f"Schema creation warning for {catalog_name}: {result.get('error')}")
        
        # Try to drop tables (ignore failures)
        for catalog_name, query in drop_queries:
            if catalog_name == "iceberg_polaris":
                conn = self.polaris_conn
            elif catalog_name == "iceberg_hms":
                conn = self.hms_conn
            else:  # iceberg_nessie
                conn = self.nessie_conn
            result = self.execute_query(conn, query, catalog_name)
            if not result['success']:
                logger.info(f"Table drop info for {catalog_name}: {result.get('error')}")
        
        # Create tables
        for catalog_name, query in create_queries:
            if catalog_name == "iceberg_polaris":
                conn = self.polaris_conn
            elif catalog_name == "iceberg_hms":
                conn = self.hms_conn
            else:  # iceberg_nessie
                conn = self.nessie_conn
            result = self.execute_query(conn, query, catalog_name)
            if not result['success']:
                logger.error(f"Table creation failed for {catalog_name}: {result.get('error')}")
                raise Exception(f"Failed to create table in {catalog_name}")

    def generate_stress_insert_query(self, table_name: str, batch_size: int, batch_id: int, thread_id: str) -> str:
        """Generate INSERT query with stress test data"""
        values = []
        current_date = time.strftime('%Y-%m-%d')
        
        for i in range(batch_size):
            id_val = random.randint(1, 10000000)
            name_val = f"'stress_test_{id_val}'"
            value_val = round(random.uniform(1.0, 10000.0), 2)
            
            # Generate random data to simulate real-world scenarios
            random_data = f"'{''.join(random.choices('abcdefghijklmnopqrstuvwxyz0123456789', k=random.randint(10, 100)))}'"
            
            created_at = f"TIMESTAMP '{current_date} {random.randint(0,23):02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}'"
            partition_date = f"DATE '{current_date}'"
            
            values.append(f"({id_val}, {batch_id}, '{thread_id}', {name_val}, {value_val}, {random_data}, {created_at}, {partition_date})")
        
        return f"INSERT INTO {table_name} VALUES {', '.join(values)}"

    def stress_insert_worker(self, catalog_name: str, thread_num: int, batches: int, batch_size: int, delay_range: tuple):
        """Worker function for stress testing inserts"""
        if catalog_name == "iceberg_polaris":
            conn = self.polaris_conn
        elif catalog_name == "iceberg_hms":
            conn = self.hms_conn
        else:  # iceberg_nessie
            conn = self.nessie_conn
            
        results = []
        thread_id = f"{catalog_name}-ST{thread_num}"
        
        logger.info(f"[{thread_id}] Starting stress test: {batches} batches of {batch_size} rows each")
        
        for batch_id in range(batches):
            try:
                query = self.generate_stress_insert_query(
                    f"stress_test.{self.table_name}", 
                    batch_size, 
                    batch_id, 
                    thread_id
                )
                
                logger.info(f"[{thread_id}] Executing batch {batch_id + 1}/{batches}")
                result = self.execute_query(conn, query, thread_id)
                results.append(result)
                
                if result['success']:
                    logger.info(f"[{thread_id}] Batch {batch_id + 1} completed in {result['duration']:.2f}s")
                else:
                    logger.error(f"[{thread_id}] Batch {batch_id + 1} failed: {result.get('error')}")
                
                # Random delay between batches
                delay = random.uniform(delay_range[0], delay_range[1])
                time.sleep(delay)
                
            except Exception as e:
                logger.error(f"[{thread_id}] Unexpected error in batch {batch_id}: {e}")
                results.append({
                    'catalog': thread_id,
                    'success': False,
                    'error': str(e),
                    'duration': 0,
                    'timestamp': time.time()
                })
        
        logger.info(f"[{thread_id}] Completed all {batches} batches")
        return results

    def run_stress_test(self, threads_per_catalog=5, batches_per_thread=10, batch_size=100, delay_range=(0.1, 1.0)):
        """Run comprehensive stress test"""
        logger.info("="*80)
        logger.info("STARTING INSERT STRESS TEST")
        logger.info("="*80)
        logger.info(f"Configuration:")
        logger.info(f"  - Threads per catalog: {threads_per_catalog}")
        logger.info(f"  - Batches per thread: {batches_per_thread}")
        logger.info(f"  - Batch size: {batch_size}")
        logger.info(f"  - Total rows per catalog: {threads_per_catalog * batches_per_thread * batch_size:,}")
        logger.info(f"  - Delay range: {delay_range}")
        logger.info("="*80)
        
        # Setup test environment
        print("Setting up stress test environment...")
        self.setup_stress_environment()
        
        start_time = time.time()
        all_results = []
        
        # Create tasks for all catalogs
        tasks = []
        with ThreadPoolExecutor(max_workers=threads_per_catalog * 3) as executor:
            # Submit tasks for Polaris
            for i in range(threads_per_catalog):
                task = executor.submit(
                    self.stress_insert_worker, 
                    "iceberg_polaris", 
                    i, 
                    batches_per_thread, 
                    batch_size, 
                    delay_range
                )
                tasks.append(task)
            
            # Submit tasks for HMS
            for i in range(threads_per_catalog):
                task = executor.submit(
                    self.stress_insert_worker, 
                    "iceberg_hms", 
                    i, 
                    batches_per_thread, 
                    batch_size, 
                    delay_range
                )
                tasks.append(task)
            
            # Submit tasks for Nessie
            for i in range(threads_per_catalog):
                task = executor.submit(
                    self.stress_insert_worker, 
                    "iceberg_nessie", 
                    i, 
                    batches_per_thread, 
                    batch_size, 
                    delay_range
                )
                tasks.append(task)
            
            # Collect results
            for task in as_completed(tasks):
                try:
                    results = task.result()
                    all_results.extend(results)
                except Exception as e:
                    logger.error(f"Task failed: {e}")
        
        end_time = time.time()
        total_duration = end_time - start_time
        
        # Analyze results
        self.analyze_stress_results(all_results, total_duration)
        
        return all_results

    def analyze_stress_results(self, results: List[Dict[str, Any]], total_duration: float):
        """Analyze and print stress test results"""
        # Separate results by catalog
        polaris_results = [r for r in results if 'iceberg_polaris-ST' in r['catalog']]
        hms_results = [r for r in results if 'iceberg_hms-ST' in r['catalog']]
        nessie_results = [r for r in results if 'iceberg_nessie-ST' in r['catalog']]
        
        # Calculate success/failure counts
        polaris_success = [r for r in polaris_results if r['success']]
        polaris_failed = [r for r in polaris_results if not r['success']]
        hms_success = [r for r in hms_results if r['success']]
        hms_failed = [r for r in hms_results if not r['success']]
        nessie_success = [r for r in nessie_results if r['success']]
        nessie_failed = [r for r in nessie_results if not r['success']]
        
        print("\n" + "="*80)
        print("STRESS TEST RESULTS")
        print("="*80)
        print(f"Total test duration: {total_duration:.2f}s")
        
        # Polaris results
        print(f"\nPOLARIS CATALOG (iceberg_polaris):")
        print(f"  Total operations: {len(polaris_results)}")
        if polaris_results:
            print(f"  Successful: {len(polaris_success)} ({len(polaris_success)/len(polaris_results)*100:.1f}%)")
            print(f"  Failed: {len(polaris_failed)} ({len(polaris_failed)/len(polaris_results)*100:.1f}%)")
        
        if polaris_success:
            durations = [r['duration'] for r in polaris_success]
            total_rows = sum(r.get('rows_count', 0) for r in polaris_success)
            print(f"  Total rows inserted: {total_rows:,}")
            print(f"  Average duration: {statistics.mean(durations):.2f}s")
            print(f"  Median duration: {statistics.median(durations):.2f}s")
            print(f"  Min duration: {min(durations):.2f}s")
            print(f"  Max duration: {max(durations):.2f}s")
            print(f"  Std deviation: {statistics.stdev(durations):.2f}s")
            print(f"  Throughput: {total_rows/total_duration:.1f} rows/sec")
        
        # HMS results
        # HMS results
        print(f"\nHIVE METASTORE CATALOG (iceberg_hms):")
        print(f"  Total operations: {len(hms_results)}")
        if hms_results:
            print(f"  Successful: {len(hms_success)} ({len(hms_success)/len(hms_results)*100:.1f}%)")
            print(f"  Failed: {len(hms_failed)} ({len(hms_failed)/len(hms_results)*100:.1f}%)")
        
        if hms_success:
            durations = [r['duration'] for r in hms_success]
            total_rows = sum(r.get('rows_count', 0) for r in hms_success)
            print(f"  Total rows inserted: {total_rows:,}")
            print(f"  Average duration: {statistics.mean(durations):.2f}s")
            print(f"  Median duration: {statistics.median(durations):.2f}s")
            print(f"  Min duration: {min(durations):.2f}s")
            print(f"  Max duration: {max(durations):.2f}s")
            print(f"  Std deviation: {statistics.stdev(durations):.2f}s")
            print(f"  Throughput: {total_rows/total_duration:.1f} rows/sec")
        
        # Nessie results
        print(f"\nNESSIE CATALOG (iceberg_nessie):")
        print(f"  Total operations: {len(nessie_results)}")
        if nessie_results:
            print(f"  Successful: {len(nessie_success)} ({len(nessie_success)/len(nessie_results)*100:.1f}%)")
            print(f"  Failed: {len(nessie_failed)} ({len(nessie_failed)/len(nessie_results)*100:.1f}%)")
        
        if nessie_success:
            durations = [r['duration'] for r in nessie_success]
            total_rows = sum(r.get('rows_count', 0) for r in nessie_success)
            print(f"  Total rows inserted: {total_rows:,}")
            print(f"  Average duration: {statistics.mean(durations):.2f}s")
            print(f"  Median duration: {statistics.median(durations):.2f}s")
            print(f"  Min duration: {min(durations):.2f}s")
            print(f"  Max duration: {max(durations):.2f}s")
            print(f"  Std deviation: {statistics.stdev(durations):.2f}s")
            print(f"  Throughput: {total_rows/total_duration:.1f} rows/sec")
        
        # Performance comparison
        catalogs_data = []
        if polaris_success:
            catalogs_data.append(("Polaris", statistics.mean([r['duration'] for r in polaris_success])))
        if hms_success:
            catalogs_data.append(("HMS", statistics.mean([r['duration'] for r in hms_success])))
        if nessie_success:
            catalogs_data.append(("Nessie", statistics.mean([r['duration'] for r in nessie_success])))
        
        if len(catalogs_data) > 1:
            print(f"\nPERFORMANCE COMPARISON:")
            catalogs_data.sort(key=lambda x: x[1])  # Sort by average duration
            fastest = catalogs_data[0]
            print(f"  Fastest: {fastest[0]} ({fastest[1]:.2f}s average)")
            for i, (name, avg_duration) in enumerate(catalogs_data[1:], 1):
                slower_pct = ((avg_duration - fastest[1]) / fastest[1]) * 100
                print(f"  {name}: {slower_pct:.1f}% slower than {fastest[0]}")
        
        # Print failed operations
        if polaris_failed:
            print(f"\nFAILED POLARIS OPERATIONS:")
            for r in polaris_failed[:5]:  # Show first 5 failures
                print(f"  - {r['catalog']}: {r.get('error', 'Unknown error')}")
            if len(polaris_failed) > 5:
                print(f"  ... and {len(polaris_failed) - 5} more failures")
        
        if hms_failed:
            print(f"\nFAILED HMS OPERATIONS:")
            for r in hms_failed[:5]:  # Show first 5 failures
                print(f"  - {r['catalog']}: {r.get('error', 'Unknown error')}")
            if len(hms_failed) > 5:
                print(f"  ... and {len(hms_failed) - 5} more failures")
        
        if nessie_failed:
            print(f"\nFAILED NESSIE OPERATIONS:")
            for r in nessie_failed[:5]:  # Show first 5 failures
                print(f"  - {r['catalog']}: {r.get('error', 'Unknown error')}")
            if len(nessie_failed) > 5:
                print(f"  ... and {len(nessie_failed) - 5} more failures")
        
        print("\n" + "="*80)

    def cleanup(self):
        """Clean up connections"""
        if self.polaris_conn:
            self.polaris_conn.close()
        if self.hms_conn:
            self.hms_conn.close()
        if self.nessie_conn:
            self.nessie_conn.close()

def main():
    """Main function to run the stress tests"""
    print("INSERT STRESS TEST FOR POLARIS vs HIVE METASTORE vs NESSIE")
    print("="*60)
    
    # Test configurations
    test_configs = [
        {
            'name': 'Light Load',
            'threads_per_catalog': 3,
            'batches_per_thread': 5,
            'batch_size': 50,
            'delay_range': (0.5, 1.5)
        },
        {
            'name': 'Medium Load', 
            'threads_per_catalog': 5,
            'batches_per_thread': 8,
            'batch_size': 100,
            'delay_range': (0.2, 1.0)
        },
        {
            'name': 'Heavy Load',
            'threads_per_catalog': 8,
            'batches_per_thread': 10,
            'batch_size': 150,
            'delay_range': (0.1, 0.5)
        }
    ]
    
    print("Available stress test configurations:")
    for i, config in enumerate(test_configs):
        total_rows_per_catalog = config['threads_per_catalog'] * config['batches_per_thread'] * config['batch_size']
        total_rows_all = total_rows_per_catalog * 3  # 3 catalogs now
        print(f"{i+1}. {config['name']}: {total_rows_per_catalog:,} rows per catalog ({total_rows_all:,} total)")
    
    print("4. Custom configuration")
    
    choice = input("\nSelect test configuration (1-4): ").strip()
    
    if choice == "4":
        # Custom configuration
        threads = int(input("Threads per catalog: "))
        batches = int(input("Batches per thread: "))
        batch_size = int(input("Batch size: "))
        min_delay = float(input("Min delay between batches (seconds): "))
        max_delay = float(input("Max delay between batches (seconds): "))
        
        config = {
            'threads_per_catalog': threads,
            'batches_per_thread': batches,
            'batch_size': batch_size,
            'delay_range': (min_delay, max_delay)
        }
    elif choice in ["1", "2", "3"]:
        config = test_configs[int(choice) - 1]
        # Remove the 'name' key as it's not needed for the function call
        config = {k: v for k, v in config.items() if k != 'name'}
    else:
        print("Invalid choice, using Medium Load configuration")
        config = test_configs[1]
        config = {k: v for k, v in config.items() if k != 'name'}
    
    tester = None
    try:
        print(f"\nInitializing stress tester...")
        tester = InsertStressTester()
        
        # Run stress test
        results = tester.run_stress_test(**config)
        
        total_ops = len(results)
        successful_ops = len([r for r in results if r['success']])
        print(f"\nStress test completed!")
        print(f"Total operations: {total_ops}")
        print(f"Successful operations: {successful_ops} ({successful_ops/total_ops*100:.1f}%)")
        
    except Exception as e:
        logger.error(f"Stress test failed: {e}")
        raise
    finally:
        if tester:
            tester.cleanup()

if __name__ == "__main__":
    main()
