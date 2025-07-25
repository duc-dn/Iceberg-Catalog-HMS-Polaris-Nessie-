#!/usr/bin/env python3
"""
Stress test for UPDATE operations on Polaris, Hive Metastore, and Nessie catalogs
This script performs intensive concurrent update operations to test the limits
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

class UpdateStressTester:
    def __init__(self, host='localhost', port=8081, user='admin'):
        self.host = host
        self.port = port
        self.user = user
        self.polaris_conn = None
        self.hms_conn = None
        self.nessie_conn = None
        # Use timestamp to create unique table names
        self.table_suffix = str(int(time.time()))
        self.table_name = f"update_stress_{self.table_suffix}"
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
            
            # For UPDATE queries, get the number of affected rows
            if query.strip().upper().startswith('UPDATE'):
                # For Iceberg tables, we need to commit the transaction
                connection.commit()
                rows_count = cursor.rowcount if hasattr(cursor, 'rowcount') else 0
            elif query.strip().upper().startswith('SELECT'):
                results = cursor.fetchall()
                rows_count = len(results)
            else:
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

    def setup_stress_environment(self, initial_rows=10000):
        """Setup schemas, tables and initial data for stress testing"""
        logger.info(f"Setting up stress test environment with {initial_rows:,} initial rows per catalog")
        
        setup_queries = [
            # Schema creation
            ("iceberg_polaris", "CREATE SCHEMA IF NOT EXISTS stress_test"),
            ("iceberg_hms", "CREATE SCHEMA IF NOT EXISTS stress_test"),
            ("iceberg_nessie", "CREATE SCHEMA IF NOT EXISTS stress_test"),
        ]
        
        # Try to drop existing tables
        drop_queries = [
            ("iceberg_polaris", f"DROP TABLE IF EXISTS stress_test.{self.table_name}"),
            ("iceberg_hms", f"DROP TABLE IF EXISTS stress_test.{self.table_name}"),
            ("iceberg_nessie", f"DROP TABLE IF EXISTS stress_test.{self.table_name}")
        ]
        
        create_queries = [
            # Table creation
            ("iceberg_polaris", f"""
                CREATE TABLE stress_test.{self.table_name} (
                    id bigint,
                    category varchar,
                    name varchar,
                    value double,
                    status varchar,
                    priority integer,
                    last_updated timestamp,
                    version integer,
                    partition_date date
                ) WITH (
                    partitioning = ARRAY['partition_date']
                )
            """),
            
            ("iceberg_hms", f"""
                CREATE TABLE stress_test.{self.table_name} (
                    id bigint,
                    category varchar,
                    name varchar,
                    value double,
                    status varchar,
                    priority integer,
                    last_updated timestamp,
                    version integer,
                    partition_date date
                ) WITH (
                    partitioning = ARRAY['partition_date']
                )
            """),
            
            ("iceberg_nessie", f"""
                CREATE TABLE stress_test.{self.table_name} (
                    id bigint,
                    category varchar,
                    name varchar,
                    value double,
                    status varchar,
                    priority integer,
                    last_updated timestamp,
                    version integer,
                    partition_date date
                ) WITH (
                    partitioning = ARRAY['partition_date']
                )
            """)
        ]
        
        # Execute setup
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
        
        # Drop tables
        for catalog_name, query in drop_queries:
            if catalog_name == "iceberg_polaris":
                conn = self.polaris_conn
            elif catalog_name == "iceberg_hms":
                conn = self.hms_conn
            else:  # iceberg_nessie
                conn = self.nessie_conn
            result = self.execute_query(conn, query, catalog_name)
        
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
        
        # Insert initial data
        self.populate_initial_data(initial_rows)

    def populate_initial_data(self, num_rows: int):
        """Populate tables with initial data for update testing"""
        logger.info(f"Populating initial data: {num_rows:,} rows per catalog")
        
        batch_size = 1000
        current_date = time.strftime('%Y-%m-%d')
        categories = ['Electronics', 'Books', 'Clothing', 'Home', 'Sports', 'Toys']
        statuses = ['Active', 'Pending', 'Inactive', 'Processing']
        
        for catalog in ['iceberg_polaris', 'iceberg_hms', 'iceberg_nessie']:
            if catalog == "iceberg_polaris":
                conn = self.polaris_conn
            elif catalog == "iceberg_hms":
                conn = self.hms_conn
            else:  # iceberg_nessie
                conn = self.nessie_conn
            
            for batch_start in range(0, num_rows, batch_size):
                batch_end = min(batch_start + batch_size, num_rows)
                values = []
                
                for i in range(batch_start, batch_end):
                    id_val = i + 1
                    category = random.choice(categories)
                    name = f"'item_{id_val}'"
                    value = round(random.uniform(10.0, 1000.0), 2)
                    status = random.choice(statuses)
                    priority = random.randint(1, 5)
                    last_updated = f"TIMESTAMP '{current_date} {random.randint(0,23):02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}'"
                    version = 1
                    partition_date = f"DATE '{current_date}'"
                    
                    values.append(f"({id_val}, '{category}', {name}, {value}, '{status}', {priority}, {last_updated}, {version}, {partition_date})")
                
                insert_query = f"INSERT INTO stress_test.{self.table_name} VALUES {', '.join(values)}"
                result = self.execute_query(conn, insert_query, f"{catalog}_initial_data")
                
                if not result['success']:
                    logger.error(f"Failed to insert initial data batch for {catalog}: {result.get('error')}")
                    raise Exception(f"Failed to populate initial data in {catalog}")
                
                logger.info(f"[{catalog}] Inserted batch {batch_start + 1}-{batch_end}")

    def generate_update_queries(self, thread_id: str, num_updates: int) -> List[str]:
        """Generate various types of UPDATE queries"""
        queries = []
        categories = ['Electronics', 'Books', 'Clothing', 'Home', 'Sports', 'Toys']
        statuses = ['Active', 'Pending', 'Inactive', 'Processing', 'Completed']
        current_timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        
        for i in range(num_updates):
            update_type = random.choice([
                'value_update',
                'status_update', 
                'priority_update',
                'category_update',
                'version_increment',
                'bulk_update'
            ])
            
            if update_type == 'value_update':
                # Update value for specific IDs
                start_id = random.randint(1, 9000)
                end_id = start_id + random.randint(1, 100)
                new_value = round(random.uniform(10.0, 2000.0), 2)
                query = f"""
                    UPDATE stress_test.{self.table_name} 
                    SET value = {new_value}, 
                        last_updated = TIMESTAMP '{current_timestamp}',
                        version = version + 1
                    WHERE id BETWEEN {start_id} AND {end_id}
                """
                
            elif update_type == 'status_update':
                # Update status for a category
                category = random.choice(categories)
                new_status = random.choice(statuses)
                query = f"""
                    UPDATE stress_test.{self.table_name} 
                    SET status = '{new_status}',
                        last_updated = TIMESTAMP '{current_timestamp}',
                        version = version + 1
                    WHERE category = '{category}' AND priority > {random.randint(1, 3)}
                """
                
            elif update_type == 'priority_update':
                # Update priority based on value
                threshold = random.randint(100, 800)
                new_priority = random.randint(1, 5)
                query = f"""
                    UPDATE stress_test.{self.table_name} 
                    SET priority = {new_priority},
                        last_updated = TIMESTAMP '{current_timestamp}',
                        version = version + 1
                    WHERE value > {threshold}
                """
                
            elif update_type == 'category_update':
                # Update category for specific status
                old_status = random.choice(statuses)
                new_category = random.choice(categories)
                query = f"""
                    UPDATE stress_test.{self.table_name} 
                    SET category = '{new_category}',
                        last_updated = TIMESTAMP '{current_timestamp}',
                        version = version + 1
                    WHERE status = '{old_status}' AND id % 10 = {random.randint(0, 9)}
                """
                
            elif update_type == 'version_increment':
                # Increment version for all rows with specific criteria
                min_value = random.randint(50, 500)
                query = f"""
                    UPDATE stress_test.{self.table_name} 
                    SET version = version + 1,
                        last_updated = TIMESTAMP '{current_timestamp}'
                    WHERE value > {min_value} AND priority <= 3
                """
                
            elif update_type == 'bulk_update':
                # Bulk update with multiple conditions
                category = random.choice(categories)
                status = random.choice(statuses)
                new_value_multiplier = round(random.uniform(0.8, 1.2), 2)
                query = f"""
                    UPDATE stress_test.{self.table_name} 
                    SET value = value * {new_value_multiplier},
                        status = '{status}',
                        last_updated = TIMESTAMP '{current_timestamp}',
                        version = version + 1
                    WHERE category = '{category}' AND version < 5
                """
            
            queries.append(query.strip())
        
        return queries

    def update_stress_worker(self, catalog_name: str, thread_num: int, updates_per_batch: int, num_batches: int, delay_range: tuple):
        """Worker function for stress testing updates"""
        if catalog_name == "iceberg_polaris":
            conn = self.polaris_conn
        elif catalog_name == "iceberg_hms":
            conn = self.hms_conn
        else:  # iceberg_nessie
            conn = self.nessie_conn
            
        results = []
        thread_id = f"{catalog_name}-UT{thread_num}"
        
        logger.info(f"[{thread_id}] Starting update stress test: {num_batches} batches of {updates_per_batch} updates each")
        
        for batch_id in range(num_batches):
            try:
                # Generate update queries for this batch
                update_queries = self.generate_update_queries(thread_id, updates_per_batch)
                
                logger.info(f"[{thread_id}] Executing batch {batch_id + 1}/{num_batches} ({len(update_queries)} updates)")
                
                batch_start_time = time.time()
                batch_results = []
                
                for query_idx, query in enumerate(update_queries):
                    result = self.execute_query(conn, query, thread_id)
                    batch_results.append(result)
                    
                    if not result['success']:
                        logger.warning(f"[{thread_id}] Update {query_idx + 1} failed: {result.get('error')}")
                
                batch_duration = time.time() - batch_start_time
                successful_updates = len([r for r in batch_results if r['success']])
                total_rows_affected = sum(r.get('rows_count', 0) for r in batch_results if r['success'])
                
                logger.info(f"[{thread_id}] Batch {batch_id + 1} completed: {successful_updates}/{len(update_queries)} updates successful, {total_rows_affected} rows affected, {batch_duration:.2f}s")
                
                results.extend(batch_results)
                
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
        
        logger.info(f"[{thread_id}] Completed all {num_batches} batches")
        return results

    def run_update_stress_test(self, initial_rows=10000, threads_per_catalog=3, updates_per_batch=5, num_batches=10, delay_range=(0.2, 1.0)):
        """Run comprehensive update stress test"""
        logger.info("="*80)
        logger.info("STARTING UPDATE STRESS TEST")
        logger.info("="*80)
        logger.info(f"Configuration:")
        logger.info(f"  - Initial rows per catalog: {initial_rows:,}")
        logger.info(f"  - Threads per catalog: {threads_per_catalog}")
        logger.info(f"  - Updates per batch: {updates_per_batch}")
        logger.info(f"  - Batches per thread: {num_batches}")
        logger.info(f"  - Total updates per catalog: {threads_per_catalog * updates_per_batch * num_batches:,}")
        logger.info(f"  - Total updates across all catalogs: {3 * threads_per_catalog * updates_per_batch * num_batches:,}")
        logger.info(f"  - Delay range: {delay_range}")
        logger.info("="*80)
        
        # Setup test environment
        print("Setting up update stress test environment...")
        self.setup_stress_environment(initial_rows)
        
        start_time = time.time()
        all_results = []
        
        # Create tasks for all catalogs
        tasks = []
        with ThreadPoolExecutor(max_workers=threads_per_catalog * 3) as executor:
            # Submit tasks for Polaris
            for i in range(threads_per_catalog):
                task = executor.submit(
                    self.update_stress_worker, 
                    "iceberg_polaris", 
                    i, 
                    updates_per_batch,
                    num_batches, 
                    delay_range
                )
                tasks.append(task)
            
            # Submit tasks for HMS
            for i in range(threads_per_catalog):
                task = executor.submit(
                    self.update_stress_worker, 
                    "iceberg_hms", 
                    i, 
                    updates_per_batch,
                    num_batches, 
                    delay_range
                )
                tasks.append(task)
            
            # Submit tasks for Nessie
            for i in range(threads_per_catalog):
                task = executor.submit(
                    self.update_stress_worker, 
                    "iceberg_nessie", 
                    i, 
                    updates_per_batch,
                    num_batches, 
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
        self.analyze_update_results(all_results, total_duration)
        
        return all_results

    def analyze_update_results(self, results: List[Dict[str, Any]], total_duration: float):
        """Analyze and print update stress test results"""
        # Separate results by catalog
        polaris_results = [r for r in results if 'iceberg_polaris-UT' in r['catalog']]
        hms_results = [r for r in results if 'iceberg_hms-UT' in r['catalog']]
        nessie_results = [r for r in results if 'iceberg_nessie-UT' in r['catalog']]
        
        # Calculate success/failure counts
        polaris_success = [r for r in polaris_results if r['success']]
        polaris_failed = [r for r in polaris_results if not r['success']]
        hms_success = [r for r in hms_results if r['success']]
        hms_failed = [r for r in hms_results if not r['success']]
        nessie_success = [r for r in nessie_results if r['success']]
        nessie_failed = [r for r in nessie_results if not r['success']]
        
        print("\n" + "="*80)
        print("UPDATE STRESS TEST RESULTS")
        print("="*80)
        print(f"Total test duration: {total_duration:.2f}s")
        
        # Polaris results
        print(f"\nPOLARIS CATALOG (iceberg_polaris):")
        print(f"  Total update operations: {len(polaris_results)}")
        if polaris_results:
            print(f"  Successful: {len(polaris_success)} ({len(polaris_success)/len(polaris_results)*100:.1f}%)")
            print(f"  Failed: {len(polaris_failed)} ({len(polaris_failed)/len(polaris_results)*100:.1f}%)")
        
        if polaris_success:
            durations = [r['duration'] for r in polaris_success]
            total_rows_affected = sum(r.get('rows_count', 0) for r in polaris_success)
            print(f"  Total rows affected: {total_rows_affected:,}")
            print(f"  Average duration: {statistics.mean(durations):.3f}s")
            print(f"  Median duration: {statistics.median(durations):.3f}s")
            print(f"  Min duration: {min(durations):.3f}s")
            print(f"  Max duration: {max(durations):.3f}s")
            if len(durations) > 1:
                print(f"  Std deviation: {statistics.stdev(durations):.3f}s")
            print(f"  Updates per second: {len(polaris_success)/total_duration:.1f}")
            if total_rows_affected > 0:
                print(f"  Rows affected per second: {total_rows_affected/total_duration:.1f}")
        
        # HMS results
        print(f"\nHIVE METASTORE CATALOG (iceberg_hms):")
        print(f"  Total update operations: {len(hms_results)}")
        print(f"  Successful: {len(hms_success)} ({len(hms_success)/len(hms_results)*100:.1f}%)")
        print(f"  Failed: {len(hms_failed)} ({len(hms_failed)/len(hms_results)*100:.1f}%)")
        
        if hms_success:
            durations = [r['duration'] for r in hms_success]
            total_rows_affected = sum(r.get('rows_count', 0) for r in hms_success)
            print(f"  Total rows affected: {total_rows_affected:,}")
            print(f"  Average duration: {statistics.mean(durations):.3f}s")
            print(f"  Median duration: {statistics.median(durations):.3f}s")
            print(f"  Min duration: {min(durations):.3f}s")
            print(f"  Max duration: {max(durations):.3f}s")
            if len(durations) > 1:
                print(f"  Std deviation: {statistics.stdev(durations):.3f}s")
            print(f"  Updates per second: {len(hms_success)/total_duration:.1f}")
            if total_rows_affected > 0:
                print(f"  Rows affected per second: {total_rows_affected/total_duration:.1f}")
        
        # Nessie results
        print(f"\nNESSIE CATALOG (iceberg_nessie):")
        print(f"  Total update operations: {len(nessie_results)}")
        if nessie_results:
            print(f"  Successful: {len(nessie_success)} ({len(nessie_success)/len(nessie_results)*100:.1f}%)")
            print(f"  Failed: {len(nessie_failed)} ({len(nessie_failed)/len(nessie_results)*100:.1f}%)")
        
        if nessie_success:
            durations = [r['duration'] for r in nessie_success]
            total_rows_affected = sum(r.get('rows_count', 0) for r in nessie_success)
            print(f"  Total rows affected: {total_rows_affected:,}")
            print(f"  Average duration: {statistics.mean(durations):.3f}s")
            print(f"  Median duration: {statistics.median(durations):.3f}s")
            print(f"  Min duration: {min(durations):.3f}s")
            print(f"  Max duration: {max(durations):.3f}s")
            if len(durations) > 1:
                print(f"  Std deviation: {statistics.stdev(durations):.3f}s")
            print(f"  Updates per second: {len(nessie_success)/total_duration:.1f}")
            if total_rows_affected > 0:
                print(f"  Rows affected per second: {total_rows_affected/total_duration:.1f}")
        
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
            print(f"  Fastest: {fastest[0]} ({fastest[1]:.3f}s average)")
            for i, (name, avg_duration) in enumerate(catalogs_data[1:], 1):
                slower_pct = ((avg_duration - fastest[1]) / fastest[1]) * 100
                print(f"  {name}: {slower_pct:.1f}% slower than {fastest[0]}")
        
        # Print sample failures
        if polaris_failed:
            print(f"\nSAMPLE POLARIS FAILURES:")
            for r in polaris_failed[:3]:
                print(f"  - {r['catalog']}: {r.get('error', 'Unknown error')[:100]}...")
            if len(polaris_failed) > 3:
                print(f"  ... and {len(polaris_failed) - 3} more failures")
        
        if hms_failed:
            print(f"\nSAMPLE HMS FAILURES:")
            for r in hms_failed[:3]:
                print(f"  - {r['catalog']}: {r.get('error', 'Unknown error')[:100]}...")
            if len(hms_failed) > 3:
                print(f"  ... and {len(hms_failed) - 3} more failures")
        
        if nessie_failed:
            print(f"\nSAMPLE NESSIE FAILURES:")
            for r in nessie_failed[:3]:
                print(f"  - {r['catalog']}: {r.get('error', 'Unknown error')[:100]}...")
            if len(nessie_failed) > 3:
                print(f"  ... and {len(nessie_failed) - 3} more failures")
        
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
    """Main function to run the update stress tests"""
    print("UPDATE STRESS TEST FOR POLARIS vs HIVE METASTORE vs NESSIE")
    print("="*70)
    
    # Test configurations
    test_configs = [
        {
            'name': 'Light Update Load',
            'initial_rows': 5000,
            'threads_per_catalog': 2,
            'updates_per_batch': 3,
            'num_batches': 5,
            'delay_range': (0.5, 1.5)
        },
        {
            'name': 'Medium Update Load', 
            'initial_rows': 10000,
            'threads_per_catalog': 3,
            'updates_per_batch': 5,
            'num_batches': 8,
            'delay_range': (0.2, 1.0)
        },
        {
            'name': 'Heavy Update Load',
            'initial_rows': 20000,
            'threads_per_catalog': 5,
            'updates_per_batch': 8,
            'num_batches': 10,
            'delay_range': (0.1, 0.5)
        }
    ]
    
    print("Available update stress test configurations:")
    for i, config in enumerate(test_configs):
        total_updates_per_catalog = config['threads_per_catalog'] * config['updates_per_batch'] * config['num_batches']
        total_updates_all = total_updates_per_catalog * 3  # 3 catalogs now
        print(f"{i+1}. {config['name']}: {config['initial_rows']:,} initial rows, {total_updates_per_catalog} updates per catalog ({total_updates_all} total)")
    
    print("4. Custom configuration")
    
    choice = input("\nSelect test configuration (1-4): ").strip()
    
    if choice == "4":
        # Custom configuration
        initial_rows = int(input("Initial rows per catalog: "))
        threads = int(input("Threads per catalog: "))
        updates_per_batch = int(input("Updates per batch: "))
        num_batches = int(input("Number of batches per thread: "))
        min_delay = float(input("Min delay between batches (seconds): "))
        max_delay = float(input("Max delay between batches (seconds): "))
        
        config = {
            'initial_rows': initial_rows,
            'threads_per_catalog': threads,
            'updates_per_batch': updates_per_batch,
            'num_batches': num_batches,
            'delay_range': (min_delay, max_delay)
        }
    elif choice in ["1", "2", "3"]:
        config = test_configs[int(choice) - 1]
        # Remove the 'name' key as it's not needed for the function call
        config = {k: v for k, v in config.items() if k != 'name'}
    else:
        print("Invalid choice, using Medium Update Load configuration")
        config = test_configs[1]
        config = {k: v for k, v in config.items() if k != 'name'}
    
    tester = None
    try:
        print(f"\nInitializing update stress tester...")
        tester = UpdateStressTester()
        
        # Run stress test
        results = tester.run_update_stress_test(**config)
        
        total_ops = len(results)
        successful_ops = len([r for r in results if r['success']])
        print(f"\nUpdate stress test completed!")
        print(f"Total update operations: {total_ops}")
        print(f"Successful operations: {successful_ops} ({successful_ops/total_ops*100:.1f}%)")
        
    except Exception as e:
        logger.error(f"Update stress test failed: {e}")
        raise
    finally:
        if tester:
            tester.cleanup()

if __name__ == "__main__":
    main()
