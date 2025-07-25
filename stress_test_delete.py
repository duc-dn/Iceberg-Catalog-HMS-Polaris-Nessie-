#!/usr/bin/env python3
"""
Stress test for DELETE operations on Polaris, HMS, and Nessie catalogs
This script performs intensive concurrent delete operations to test the limits
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

class DeleteStressTester:
    def __init__(self, host='localhost', port=8081, user='admin'):
        self.host = host
        self.port = port
        self.user = user
        self.polaris_conn = None
        self.hms_conn = None
        self.nessie_conn = None
        # Use timestamp to create unique table names
        self.table_suffix = str(int(time.time()))
        self.table_name = f"delete_stress_{self.table_suffix}"
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

    def setup_stress_environment(self, total_rows_per_catalog=10000):
        """Setup schemas, tables and insert test data for delete stress testing"""
        setup_queries = [
            ("iceberg_polaris", "CREATE SCHEMA IF NOT EXISTS stress_test"),
            ("iceberg_hms", "CREATE SCHEMA IF NOT EXISTS stress_test"),
            ("iceberg_nessie", "CREATE SCHEMA IF NOT EXISTS stress_test"),
        ]
        
        # Drop existing tables
        drop_queries = [
            ("iceberg_polaris", f"DROP TABLE IF EXISTS stress_test.{self.table_name}"),
            ("iceberg_hms", f"DROP TABLE IF EXISTS stress_test.{self.table_name}"),
            ("iceberg_nessie", f"DROP TABLE IF EXISTS stress_test.{self.table_name}")
        ]
        
        create_queries = [
            ("iceberg_polaris", f"""
                CREATE TABLE stress_test.{self.table_name} (
                    id bigint,
                    category varchar,
                    thread_group varchar,
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
                    category varchar,
                    thread_group varchar,
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
                    category varchar,
                    thread_group varchar,
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
        
        # Execute setup
        for catalog_name, query in setup_queries:
            conn = self._get_connection(catalog_name)
            result = self.execute_query(conn, query, catalog_name)
            if not result['success']:
                logger.warning(f"Schema creation warning for {catalog_name}: {result.get('error')}")
        
        for catalog_name, query in drop_queries:
            conn = self._get_connection(catalog_name)
            result = self.execute_query(conn, query, catalog_name)
        
        for catalog_name, query in create_queries:
            conn = self._get_connection(catalog_name)
            result = self.execute_query(conn, query, catalog_name)
            if not result['success']:
                logger.error(f"Table creation failed for {catalog_name}: {result.get('error')}")
                raise Exception(f"Failed to create table in {catalog_name}")
        
        # Insert test data for each catalog
        logger.info(f"Inserting {total_rows_per_catalog:,} test rows per catalog...")
        self._insert_test_data(total_rows_per_catalog)

    def _get_connection(self, catalog_name: str):
        """Get connection for a catalog"""
        if catalog_name == "iceberg_polaris":
            return self.polaris_conn
        elif catalog_name == "iceberg_hms":
            return self.hms_conn
        else:  # iceberg_nessie
            return self.nessie_conn

    def _insert_test_data(self, total_rows_per_catalog: int):
        """Insert test data for delete operations"""
        batch_size = 1000
        current_date = time.strftime('%Y-%m-%d')
        
        catalogs = ["iceberg_polaris", "iceberg_hms", "iceberg_nessie"]
        
        for catalog_name in catalogs:
            conn = self._get_connection(catalog_name)
            logger.info(f"Inserting test data for {catalog_name}...")
            
            for batch_start in range(0, total_rows_per_catalog, batch_size):
                batch_end = min(batch_start + batch_size, total_rows_per_catalog)
                values = []
                
                for i in range(batch_start, batch_end):
                    id_val = i + 1
                    category = f"'cat_{i % 10}'"  # 10 categories
                    thread_group = f"'group_{i % 100}'"  # 100 thread groups
                    name = f"'test_record_{id_val}'"
                    value = round(random.uniform(1.0, 1000.0), 2)
                    random_data = f"'{''.join(random.choices('abcdefghijklmnopqrstuvwxyz0123456789', k=50))}'"
                    created_at = f"TIMESTAMP '{current_date} {random.randint(0,23):02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}'"
                    partition_date = f"DATE '{current_date}'"
                    
                    values.append(f"({id_val}, {category}, {thread_group}, {name}, {value}, {random_data}, {created_at}, {partition_date})")
                
                insert_query = f"INSERT INTO stress_test.{self.table_name} VALUES {', '.join(values)}"
                result = self.execute_query(conn, insert_query, catalog_name)
                
                if not result['success']:
                    logger.error(f"Failed to insert test data batch for {catalog_name}: {result.get('error')}")
                    raise Exception(f"Failed to insert test data for {catalog_name}")
                
                logger.info(f"Inserted batch {batch_start}-{batch_end} for {catalog_name}")

    def generate_delete_query(self, thread_id: str, delete_type: str) -> str:
        """Generate different types of DELETE queries"""
        table_name = f"stress_test.{self.table_name}"
        
        if delete_type == "by_id_range":
            # Delete by ID range
            start_id = random.randint(1, 9000)
            end_id = start_id + random.randint(10, 100)
            return f"DELETE FROM {table_name} WHERE id BETWEEN {start_id} AND {end_id}"
        
        elif delete_type == "by_category":
            # Delete by category
            category = random.randint(0, 9)
            return f"DELETE FROM {table_name} WHERE category = 'cat_{category}'"
        
        elif delete_type == "by_thread_group":
            # Delete by thread group
            group = random.randint(0, 99)
            return f"DELETE FROM {table_name} WHERE thread_group = 'group_{group}'"
        
        elif delete_type == "by_value_range":
            # Delete by value range
            min_val = random.uniform(1.0, 500.0)
            max_val = min_val + random.uniform(50.0, 200.0)
            return f"DELETE FROM {table_name} WHERE value BETWEEN {min_val:.2f} AND {max_val:.2f}"
        
        elif delete_type == "random_sample":
            # Delete random sample by mod
            mod_val = random.randint(50, 200)
            remainder = random.randint(0, mod_val - 1)
            return f"DELETE FROM {table_name} WHERE id % {mod_val} = {remainder}"
        
        else:
            # Default: delete by single ID
            id_val = random.randint(1, 10000)
            return f"DELETE FROM {table_name} WHERE id = {id_val}"

    def stress_delete_worker(self, catalog_name: str, thread_num: int, delete_operations: int, delay_range: tuple):
        """Worker function for stress testing deletes"""
        conn = self._get_connection(catalog_name)
        results = []
        thread_id = f"{catalog_name}-DT{thread_num}"
        
        delete_types = ["by_id_range", "by_category", "by_thread_group", "by_value_range", "random_sample"]
        
        logger.info(f"[{thread_id}] Starting delete stress test: {delete_operations} delete operations")
        
        for op_num in range(delete_operations):
            try:
                # Choose random delete type
                delete_type = random.choice(delete_types)
                query = self.generate_delete_query(thread_id, delete_type)
                
                logger.info(f"[{thread_id}] Executing delete operation {op_num + 1}/{delete_operations} ({delete_type})")
                result = self.execute_query(conn, query, thread_id)
                result['delete_type'] = delete_type
                result['operation_num'] = op_num + 1
                results.append(result)
                
                if result['success']:
                    rows_deleted = result.get('rows_count', 0)
                    logger.info(f"[{thread_id}] Delete {op_num + 1} completed in {result['duration']:.2f}s - {rows_deleted} rows deleted")
                else:
                    logger.error(f"[{thread_id}] Delete {op_num + 1} failed: {result.get('error')}")
                
                # Random delay between operations
                if op_num < delete_operations - 1:
                    delay = random.uniform(*delay_range)
                    time.sleep(delay)
                    
            except Exception as e:
                logger.error(f"[{thread_id}] Exception in delete operation {op_num}: {e}")
                results.append({
                    'catalog': thread_id,
                    'success': False,
                    'error': str(e),
                    'duration': 0,
                    'timestamp': time.time(),
                    'delete_type': 'error',
                    'operation_num': op_num + 1
                })
        
        logger.info(f"[{thread_id}] Completed all {delete_operations} delete operations")
        return results

    def run_stress_test(self, threads_per_catalog=3, delete_operations_per_thread=20, delay_range=(0.1, 1.0), initial_rows=10000):
        """Run comprehensive delete stress test"""
        logger.info("="*80)
        logger.info("STARTING DELETE STRESS TEST")
        logger.info("="*80)
        logger.info(f"Configuration:")
        logger.info(f"  - Threads per catalog: {threads_per_catalog}")
        logger.info(f"  - Delete operations per thread: {delete_operations_per_thread}")
        logger.info(f"  - Total delete operations per catalog: {threads_per_catalog * delete_operations_per_thread}")
        logger.info(f"  - Initial rows per catalog: {initial_rows:,}")
        logger.info(f"  - Delay range: {delay_range}")
        logger.info("="*80)
        
        # Setup test environment with initial data
        print("Setting up delete stress test environment...")
        self.setup_stress_environment(initial_rows)
        
        start_time = time.time()
        all_results = []
        
        # Create tasks for all catalogs
        tasks = []
        with ThreadPoolExecutor(max_workers=threads_per_catalog * 3) as executor:
            # Submit tasks for all catalogs
            for catalog_name in ["iceberg_polaris", "iceberg_hms", "iceberg_nessie"]:
                for i in range(threads_per_catalog):
                    task = executor.submit(
                        self.stress_delete_worker,
                        catalog_name,
                        i + 1,
                        delete_operations_per_thread,
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
        self.analyze_delete_results(all_results, total_duration)
        
        return all_results

    def analyze_delete_results(self, results: List[Dict[str, Any]], total_duration: float):
        """Analyze and print delete stress test results"""
        # Separate results by catalog
        polaris_results = [r for r in results if 'iceberg_polaris-DT' in r['catalog']]
        hms_results = [r for r in results if 'iceberg_hms-DT' in r['catalog']]
        nessie_results = [r for r in results if 'iceberg_nessie-DT' in r['catalog']]
        
        # Calculate success/failure counts
        polaris_success = [r for r in polaris_results if r['success']]
        polaris_failed = [r for r in polaris_results if not r['success']]
        hms_success = [r for r in hms_results if r['success']]
        hms_failed = [r for r in hms_results if not r['success']]
        nessie_success = [r for r in nessie_results if r['success']]
        nessie_failed = [r for r in nessie_results if not r['success']]
        
        print("\n" + "="*80)
        print("DELETE STRESS TEST RESULTS")
        print("="*80)
        print(f"Total test duration: {total_duration:.2f}s")
        
        # Results for each catalog
        catalog_data = [
            ("POLARIS CATALOG (iceberg_polaris)", polaris_results, polaris_success, polaris_failed),
            ("HIVE METASTORE CATALOG (iceberg_hms)", hms_results, hms_success, hms_failed),
            ("NESSIE CATALOG (iceberg_nessie)", nessie_results, nessie_success, nessie_failed)
        ]
        
        performance_summary = {}
        
        for catalog_name, all_ops, success_ops, failed_ops in catalog_data:
            print(f"\n{catalog_name}:")
            print(f"  Total operations: {len(all_ops)}")
            if all_ops:
                print(f"  Successful: {len(success_ops)} ({len(success_ops)/len(all_ops)*100:.1f}%)")
                print(f"  Failed: {len(failed_ops)} ({len(failed_ops)/len(all_ops)*100:.1f}%)")
            
            if success_ops:
                durations = [r['duration'] for r in success_ops]
                total_rows_deleted = sum(r.get('rows_count', 0) for r in success_ops)
                avg_duration = statistics.mean(durations)
                
                print(f"  Total rows deleted: {total_rows_deleted:,}")
                print(f"  Average duration: {avg_duration:.2f}s")
                print(f"  Median duration: {statistics.median(durations):.2f}s")
                print(f"  Min duration: {min(durations):.2f}s")
                print(f"  Max duration: {max(durations):.2f}s")
                print(f"  Std deviation: {statistics.stdev(durations) if len(durations) > 1 else 0:.2f}s")
                print(f"  Delete throughput: {len(success_ops)/total_duration:.1f} ops/sec")
                
                # Delete type breakdown
                delete_types = {}
                for r in success_ops:
                    dt = r.get('delete_type', 'unknown')
                    if dt not in delete_types:
                        delete_types[dt] = {'count': 0, 'rows': 0}
                    delete_types[dt]['count'] += 1
                    delete_types[dt]['rows'] += r.get('rows_count', 0)
                
                print(f"  Delete type breakdown:")
                for dt, stats in delete_types.items():
                    print(f"    {dt}: {stats['count']} ops, {stats['rows']} rows")
                
                performance_summary[catalog_name] = {
                    'avg_duration': avg_duration,
                    'success_rate': len(success_ops)/len(all_ops)*100,
                    'throughput': len(success_ops)/total_duration,
                    'total_rows_deleted': total_rows_deleted
                }
            
            # Show sample failures
            if failed_ops:
                print(f"  Sample failures:")
                for r in failed_ops[:3]:
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
                print(f"    {i}. {catalog.split('(')[0].strip()}: {stats['avg_duration']:.2f}s avg, {stats['success_rate']:.1f}% success")
            
            # Throughput comparison
            print(f"\n  Throughput ranking (highest to lowest):")
            sorted_by_throughput = sorted(performance_summary.items(), key=lambda x: x[1]['throughput'], reverse=True)
            for i, (catalog, stats) in enumerate(sorted_by_throughput, 1):
                print(f"    {i}. {catalog.split('(')[0].strip()}: {stats['throughput']:.1f} ops/sec")
        
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
    """Main function to run the delete stress tests"""
    print("DELETE STRESS TEST FOR POLARIS vs HIVE METASTORE vs NESSIE")
    print("="*60)
    
    # Test configurations
    test_configs = [
        {
            'name': 'Light Load',
            'threads_per_catalog': 2,
            'delete_operations_per_thread': 10,
            'delay_range': (0.5, 1.5),
            'initial_rows': 5000
        },
        {
            'name': 'Medium Load',
            'threads_per_catalog': 3,
            'delete_operations_per_thread': 20,
            'delay_range': (0.2, 1.0),
            'initial_rows': 10000
        },
        {
            'name': 'Heavy Load',
            'threads_per_catalog': 5,
            'delete_operations_per_thread': 30,
            'delay_range': (0.1, 0.5),
            'initial_rows': 15000
        }
    ]
    
    print("Available delete stress test configurations:")
    for i, config in enumerate(test_configs):
        total_ops_per_catalog = config['threads_per_catalog'] * config['delete_operations_per_thread']
        total_ops_all = total_ops_per_catalog * 3  # 3 catalogs
        print(f"{i+1}. {config['name']}: {total_ops_per_catalog} delete ops per catalog ({total_ops_all} total), {config['initial_rows']:,} initial rows")
    
    print("4. Custom configuration")
    
    choice = input("\nSelect test configuration (1-4): ").strip()
    
    if choice == "4":
        # Custom configuration
        threads = int(input("Threads per catalog: "))
        delete_ops = int(input("Delete operations per thread: "))
        initial_rows = int(input("Initial rows per catalog: "))
        min_delay = float(input("Min delay between operations (seconds): "))
        max_delay = float(input("Max delay between operations (seconds): "))
        
        config = {
            'threads_per_catalog': threads,
            'delete_operations_per_thread': delete_ops,
            'delay_range': (min_delay, max_delay),
            'initial_rows': initial_rows
        }
    elif choice in ["1", "2", "3"]:
        config = test_configs[int(choice) - 1]
        config = {k: v for k, v in config.items() if k != 'name'}
    else:
        print("Invalid choice, using Medium Load configuration")
        config = test_configs[1]
        config = {k: v for k, v in config.items() if k != 'name'}
    
    tester = None
    try:
        print(f"\nInitializing delete stress tester...")
        tester = DeleteStressTester()
        
        # Run stress test
        results = tester.run_stress_test(**config)
        
        total_ops = len(results)
        successful_ops = len([r for r in results if r['success']])
        print(f"\nDelete stress test completed!")
        print(f"Total operations: {total_ops}")
        print(f"Successful operations: {successful_ops} ({successful_ops/total_ops*100:.1f}%)")
        
    except Exception as e:
        logger.error(f"Delete stress test failed: {e}")
        raise
    finally:
        if tester:
            tester.cleanup()

if __name__ == "__main__":
    main()
