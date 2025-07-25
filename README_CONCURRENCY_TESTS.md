# Trino Concurrency Test Suite

This directory contains scripts to test the concurrency capabilities of three Iceberg catalog implementations:

1. **Polaris Catalog (REST)** - Apache Polaris catalog using REST API
2. **Hive Metastore Catalog** - Traditional Hive Metastore with Iceberg tables  
3. **Nessie Catalog** - Project Nessie version control for data lakes

## Architecture Overview

The setup includes:
- **PostgreSQL**: Shared database backend for Polaris and Nessie
- **MariaDB**: Database for Hive Metastore
- **MinIO**: S3-compatible object storage for all catalogs
- **Trino**: Query engine with three catalog configurations
- **Polaris**: REST catalog service
- **Nessie**: Version control service for data lakes
- **Hive Metastore**: Traditional metadata service

## Files

- `test_concurrency.py` - Main test script that performs concurrent operations
- `run_concurrency_test.py` - Simple runner script with connection checks
- `test_nessie_catalog.py` - Nessie catalog connectivity test
- `stress_test_insert.py` - INSERT stress test for all catalogs
- `stress_test_update.py` - UPDATE stress test for all catalogs  
- `stress_test_delete.py` - DELETE stress test for all catalogs
- `stress_test_comprehensive.py` - Comprehensive stress test suite
- `run_stress_test_with_nessie.sh` - Complete test orchestration script
- `test_concurrency_config.ini` - Configuration file for test parameters
- `requirements.txt` - Python dependencies

## Test Scenarios

The test suite includes multiple scenarios:

1. **Concurrent Insert Test**: Multiple threads inserting data simultaneously into all catalogs
2. **Concurrent Read Test**: Multiple threads performing various SELECT queries
3. **Mixed Workload Test**: Combination of read and write operations
4. **INSERT Stress Test**: Intensive concurrent insert operations to test catalog limits
5. **UPDATE Stress Test**: Concurrent update operations testing
6. **DELETE Stress Test**: Concurrent delete operations with various patterns
7. **Comprehensive Test**: All operations combined stress testing

## Prerequisites

1. Make sure Docker services are running:
   ```bash
   docker compose -f docker-compose-final.yml up -d
   ```

2. Verify that all catalogs are available:
   - `iceberg` - Uses Polaris catalog (REST)
   - `iceberg_hms` - Uses Hive Metastore
   - `iceberg_nessie` - Uses Nessie catalog

## Installation

Install Python dependencies:
```bash
pip install -r requirements.txt
```

## Usage

### Option 1: Use the runner script (recommended)
```bash
python run_concurrency_test.py
```

This script will:
- Check Trino connectivity
- Verify required catalogs exist
- Install dependencies if needed
- Run the selected test scenario

### Option 2: Run individual stress tests directly
```bash
# Basic concurrency test
python test_concurrency.py

# INSERT stress test
python stress_test_insert.py

# UPDATE stress test  
python stress_test_update.py

# DELETE stress test
python stress_test_delete.py

# Comprehensive stress test (all operations)
python stress_test_comprehensive.py
```

### Option 3: Run complete test suite with Nessie
```bash
./run_stress_test_with_nessie.sh
```

## Test Configuration

You can modify `test_concurrency_config.ini` to adjust:
- Number of concurrent threads
- Operations per thread
- Batch sizes
- Timing delays

## Expected Output

The test will show:
- Real-time logging of operations
- Success/failure counts for each catalog
- Performance metrics (duration, throughput)
- Error details for failed operations

## Example Results

```
CONCURRENCY TEST RESULTS
================================================================================

POLARIS CATALOG:
  Total operations: 25
  Successful: 24
  Failed: 1
  Average duration: 1.23s
  Min duration: 0.45s
  Max duration: 3.12s

HIVE METASTORE CATALOG:
  Total operations: 25
  Successful: 23
  Failed: 2
  Average duration: 1.67s
  Min duration: 0.52s
  Max duration: 4.21s
```

## Troubleshooting

1. **Connection errors**: Ensure Trino is running on localhost:8081
2. **Missing catalogs**: Check docker-compose services are up
3. **Permission errors**: Verify S3/MinIO bucket access
4. **Schema errors**: The script will create test schemas automatically

## Customization

You can modify the test script to:
- Add custom queries
- Test different table structures
- Implement additional performance metrics
- Test specific failure scenarios
