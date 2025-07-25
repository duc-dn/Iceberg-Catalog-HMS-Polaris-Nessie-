#!/usr/bin/env python3
"""
Comprehensive stress test for INSERT operations on Polaris, HMS, and Nessie catalogs
This script performs intensive concurrent insert operations to test the limits of all three catalogs
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

class ComprehensiveStressTester:
    def __init__(self, host='localhost', port=8081, user='admin'):
        self.host = host
        self.port = port
        self.user = user
        self.connections = {}
        self.catalogs = ['iceberg_polaris', 'iceberg_hms', 'iceberg_nessie']
        # Use timestamp to create unique table names
        self.table_suffix = str(int(time.time()))
        self.table_name = f"insert_stress_{self.table_suffix}"
        self.setup_connections()
        
    def setup_connections(self):
        """Setup connections to all catalogs"""
        try:
            # Connection for Polaris catalog (iceberg_polaris)
            self.connections['iceberg_polaris'] = trino.dbapi.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                catalog='iceberg_polaris',
                schema='default'
            )
            logger.info("Connected to Polaris catalog (iceberg_polaris)")
            
            # Connection for HMS catalog
            self.connections['iceberg_hms'] = trino.dbapi.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                catalog='iceberg_hms',
                schema='default'
            )
            logger.info("Connected to HMS catalog (iceberg_hms)")
            
            # Connection for Nessie catalog
            self.connections['iceberg_nessie'] = trino.dbapi.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                catalog='iceberg_nessie',
                schema='default'
            )
            logger.info("Connected to Nessie catalog (iceberg_nessie)")
            
        except Exception as e:
            logger.error(f"Failed to setup connections: {e}")
            raise

    def execute_query(self, catalog_name: str, query: str) -> Dict[str, Any]:
        """Execute a query and return results with timing"""
        start_time = time.time()
        thread_id = threading.current_thread().name
        
        try:
            connection = self.connections[catalog_name]
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
            # Schema creation for all catalogs
            ("iceberg_polaris", "CREATE SCHEMA IF NOT EXISTS stress_test"),
            ("iceberg_hms", "CREATE SCHEMA IF NOT EXISTS stress_test"),
            ("iceberg_nessie", "CREATE SCHEMA IF NOT EXISTS stress_test"),
        ]
        
        # Try to drop existing tables, but don't fail if they don't exist
        drop_queries = [
            ("iceberg_polaris", f"DROP TABLE IF EXISTS stress_test.{self.table_name}"),
            ("iceberg_hms", f"DROP TABLE IF EXISTS stress_test.{self.table_name}"),
            ("iceberg_nessie", f"DROP TABLE IF EXISTS stress_test.{self.table_name}")
        ]
        
        create_queries = [
            # Table creation for all catalogs
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
            try:
                result = self.execute_query(catalog_name, query)
                if result['success']:
                    logger.info(f"Schema created for {catalog_name}")
                else:
                    logger.warning(f"Schema creation failed for {catalog_name}: {result.get('error')}")
            except Exception as e:
                logger.warning(f"Schema creation error for {catalog_name}: {e}")
        
        # Drop existing tables
        for catalog_name, query in drop_queries:
            try:
                result = self.execute_query(catalog_name, query)
                if result['success']:
                    logger.info(f"Dropped existing table for {catalog_name}")
            except Exception as e:
                logger.info(f"No existing table to drop for {catalog_name}")
        
        # Create tables
        for catalog_name, query in create_queries:
            try:
                result = self.execute_query(catalog_name, query)
                if result['success']:
                    logger.info(f"Table created for {catalog_name}")
                else:
                    logger.error(f"Table creation failed for {catalog_name}: {result.get('error')}")
                    raise Exception(f"Failed to create table for {catalog_name}")
            except Exception as e:
                logger.error(f"Table creation error for {catalog_name}: {e}")
                raise

    def generate_stress_insert_query(self, table_name: str, batch_size: int, batch_id: int, thread_id: str) -> str:
        """Generate INSERT query with random data for stress testing"""
        values = []
        base_id = batch_id * batch_size + random.randint(1, 1000000)
        
        for i in range(batch_size):
            row_id = base_id + i
            name = f"stress_test_{thread_id}_{batch_id}_{i}"
            value = random.uniform(1.0, 1000.0)
            random_data = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', k=20))
            
            # Use current timestamp and date
            values.append(
                f"({row_id}, {batch_id}, '{thread_id}', '{name}', {value:.2f}, "
                f"'{random_data}', current_timestamp, current_date)"
            )
        
        return f"INSERT INTO {table_name} VALUES {', '.join(values)}"

    def stress_insert_worker(self, catalog_name: str, thread_num: int, batches: int, batch_size: int, delay_range: tuple):
        """Worker function for stress testing inserts"""
        results = []
        thread_id = f"{catalog_name}-ST{thread_num}"
        
        logger.info(f"[{thread_id}] Starting stress test: {batches} batches of {batch_size} rows each")
        
        for batch_id in range(batches):
            try:
                # Generate INSERT query
                query = self.generate_stress_insert_query(
                    f"stress_test.{self.table_name}", batch_size, batch_id, thread_id
                )
                
                # Execute the insert
                result = self.execute_query(catalog_name, query)
                result['batch_id'] = batch_id
                result['batch_size'] = batch_size
                results.append(result)
                
                if result['success']:
                    logger.info(f"[{thread_id}] Batch {batch_id + 1}/{batches} completed in {result['duration']:.2f}s")
                else:
                    logger.error(f"[{thread_id}] Batch {batch_id + 1}/{batches} failed: {result.get('error')}")
                
                # Random delay between batches
                if batch_id < batches - 1:  # Don't delay after the last batch
                    delay = random.uniform(*delay_range)
                    time.sleep(delay)
                    
            except Exception as e:
                logger.error(f"[{thread_id}] Exception in batch {batch_id}: {e}")
                results.append({
                    'catalog': catalog_name,
                    'success': False,
                    'error': str(e),
                    'batch_id': batch_id,
                    'thread_id': thread_id,
                    'duration': 0,
                    'timestamp': time.time()
                })
        
        logger.info(f"[{thread_id}] Completed all {batches} batches")
        return results

    def run_comprehensive_stress_test(self, threads_per_catalog=5, batches_per_thread=10, batch_size=100, delay_range=(0.1, 1.0)):
        """Run comprehensive stress test on all catalogs"""
        logger.info("="*100)
        logger.info("STARTING COMPREHENSIVE INSERT STRESS TEST (POLARIS + HMS + NESSIE)")
        logger.info("="*100)
        logger.info(f"Configuration:")
        logger.info(f"  - Catalogs: {', '.join(self.catalogs)}")
        logger.info(f"  - Threads per catalog: {threads_per_catalog}")
        logger.info(f"  - Batches per thread: {batches_per_thread}")
        logger.info(f"  - Batch size: {batch_size}")
        logger.info(f"  - Total rows per catalog: {threads_per_catalog * batches_per_thread * batch_size:,}")
        logger.info(f"  - Total rows across all catalogs: {len(self.catalogs) * threads_per_catalog * batches_per_thread * batch_size:,}")
        logger.info(f"  - Delay range: {delay_range}")
        logger.info("="*100)
        
        # Setup test environment
        print("Setting up comprehensive stress test environment...")
        self.setup_stress_environment()
        
        start_time = time.time()
        all_results = []
        
        # Create tasks for all catalogs
        tasks = []
        total_threads = len(self.catalogs) * threads_per_catalog
        
        with ThreadPoolExecutor(max_workers=total_threads) as executor:
            # Submit tasks for each catalog
            for catalog_name in self.catalogs:
                for i in range(threads_per_catalog):
                    task = executor.submit(
                        self.stress_insert_worker,
                        catalog_name,
                        i + 1,
                        batches_per_thread,
                        batch_size,
                        delay_range
                    )
                    tasks.append(task)
            
            # Collect results
            for task in as_completed(tasks):
                try:
                    batch_results = task.result()
                    all_results.extend(batch_results)
                except Exception as e:
                    logger.error(f"Task execution failed: {e}")
        
        end_time = time.time()
        total_duration = end_time - start_time
        
        # Analyze results
        self.analyze_comprehensive_results(all_results, total_duration)
        
        return all_results

    def analyze_comprehensive_results(self, results: List[Dict[str, Any]], total_duration: float):
        """Analyze and print comprehensive stress test results"""
        # Separate results by catalog
        catalog_results = {}
        for catalog in self.catalogs:
            catalog_results[catalog] = [r for r in results if r['catalog'] == catalog]
        
        print("\n" + "="*100)
        print("COMPREHENSIVE STRESS TEST RESULTS")
        print("="*100)
        print(f"Total test duration: {total_duration:.2f}s")
        
        # Results for each catalog
        catalog_names = {
            'iceberg_polaris': 'POLARIS CATALOG',
            'iceberg_hms': 'HIVE METASTORE CATALOG',
            'iceberg_nessie': 'NESSIE CATALOG'
        }
        
        performance_summary = {}
        
        for catalog, display_name in catalog_names.items():
            catalog_ops = catalog_results[catalog]
            success_ops = [r for r in catalog_ops if r['success']]
            failed_ops = [r for r in catalog_ops if not r['success']]
            
            print(f"\n{display_name} ({catalog}):")
            print(f"  Total operations: {len(catalog_ops)}")
            print(f"  Successful: {len(success_ops)} ({len(success_ops)/len(catalog_ops)*100:.1f}%)")
            print(f"  Failed: {len(failed_ops)} ({len(failed_ops)/len(catalog_ops)*100:.1f}%)")
            
            if success_ops:
                durations = [r['duration'] for r in success_ops]
                total_rows = sum(r.get('rows_count', 0) for r in success_ops)
                avg_duration = statistics.mean(durations)
                
                print(f"  Total rows inserted: {total_rows:,}")
                print(f"  Average duration: {avg_duration:.2f}s")
                print(f"  Median duration: {statistics.median(durations):.2f}s")
                print(f"  Min duration: {min(durations):.2f}s")
                print(f"  Max duration: {max(durations):.2f}s")
                print(f"  Std deviation: {statistics.stdev(durations) if len(durations) > 1 else 0:.2f}s")
                print(f"  Throughput: {total_rows/total_duration:.1f} rows/sec")
                
                performance_summary[catalog] = {
                    'avg_duration': avg_duration,
                    'success_rate': len(success_ops)/len(catalog_ops)*100,
                    'throughput': total_rows/total_duration,
                    'total_rows': total_rows
                }
            
            # Show first few failures
            if failed_ops:
                print(f"  Sample failures:")
                for r in failed_ops[:3]:  # Show first 3 failures
                    print(f"    - {r.get('error', 'Unknown error')}")
                if len(failed_ops) > 3:
                    print(f"    ... and {len(failed_ops) - 3} more failures")
        
        # Performance comparison
        if len(performance_summary) > 1:
            print(f"\nPERFORMANCE COMPARISON:")
            
            # Sort by average duration (fastest first)
            sorted_catalogs = sorted(performance_summary.items(), key=lambda x: x[1]['avg_duration'])
            
            print(f"  Speed ranking (fastest to slowest):")
            for i, (catalog, stats) in enumerate(sorted_catalogs, 1):
                display_name = catalog_names[catalog]
                print(f"    {i}. {display_name}: {stats['avg_duration']:.2f}s avg, {stats['success_rate']:.1f}% success")
            
            # Throughput comparison
            print(f"\n  Throughput ranking (highest to lowest):")
            sorted_by_throughput = sorted(performance_summary.items(), key=lambda x: x[1]['throughput'], reverse=True)
            for i, (catalog, stats) in enumerate(sorted_by_throughput, 1):
                display_name = catalog_names[catalog]
                print(f"    {i}. {display_name}: {stats['throughput']:.1f} rows/sec")
            
            # Success rate comparison
            print(f"\n  Reliability ranking (highest to lowest success rate):")
            sorted_by_success = sorted(performance_summary.items(), key=lambda x: x[1]['success_rate'], reverse=True)
            for i, (catalog, stats) in enumerate(sorted_by_success, 1):
                display_name = catalog_names[catalog]
                print(f"    {i}. {display_name}: {stats['success_rate']:.1f}% success rate")
        
        print("\n" + "="*100)

    def cleanup(self):
        """Clean up connections"""
        for catalog, conn in self.connections.items():
            if conn:
                try:
                    conn.close()
                    logger.info(f"Closed connection to {catalog}")
                except Exception as e:
                    logger.warning(f"Error closing connection to {catalog}: {e}")

def main():
    """Main function to run the comprehensive stress tests"""
    print("COMPREHENSIVE INSERT STRESS TEST FOR POLARIS vs HMS vs NESSIE")
    print("="*80)
    
    # Test configurations
    test_configs = [
        {
            'name': 'Light Load',
            'threads_per_catalog': 2,
            'batches_per_thread': 3,
            'batch_size': 30,
            'delay_range': (0.5, 1.5)
        },
        {
            'name': 'Medium Load', 
            'threads_per_catalog': 3,
            'batches_per_thread': 5,
            'batch_size': 50,
            'delay_range': (0.2, 1.0)
        },
        {
            'name': 'Heavy Load',
            'threads_per_catalog': 5,
            'batches_per_thread': 8,
            'batch_size': 80,
            'delay_range': (0.1, 0.5)
        }
    ]
    
    print("Available stress test configurations:")
    for i, config in enumerate(test_configs):
        total_rows_per_catalog = config['threads_per_catalog'] * config['batches_per_thread'] * config['batch_size']
        total_rows_all = total_rows_per_catalog * 3  # 3 catalogs
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
        print(f"\nInitializing comprehensive stress tester...")
        tester = ComprehensiveStressTester()
        
        # Run comprehensive stress test
        results = tester.run_comprehensive_stress_test(**config)
        
        total_ops = len(results)
        successful_ops = len([r for r in results if r['success']])
        print(f"\nComprehensive stress test completed!")
        print(f"Total operations: {total_ops}")
        print(f"Successful operations: {successful_ops} ({successful_ops/total_ops*100:.1f}%)")
        
    except Exception as e:
        logger.error(f"Comprehensive stress test failed: {e}")
        raise
    finally:
        if tester:
            tester.cleanup()

if __name__ == "__main__":
    main()
