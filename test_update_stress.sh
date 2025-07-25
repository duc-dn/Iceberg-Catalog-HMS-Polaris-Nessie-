#!/bin/bash

echo "TESTING UPDATE STRESS TEST WITH NESSIE"
echo "======================================"

# Test if all catalogs are accessible
echo "Testing catalog connectivity..."

echo "1. Testing Polaris (iceberg_polaris)..."
python3 -c "
import trino
try:
    conn = trino.dbapi.connect(host='localhost', port=8081, user='admin', catalog='iceberg_polaris', schema='default')
    cursor = conn.cursor()
    cursor.execute('SHOW SCHEMAS')
    print('✓ Polaris connected')
    conn.close()
except Exception as e:
    print(f'✗ Polaris failed: {e}')
"

echo "2. Testing HMS (iceberg_hms)..."
python3 -c "
import trino
try:
    conn = trino.dbapi.connect(host='localhost', port=8081, user='admin', catalog='iceberg_hms', schema='default')
    cursor = conn.cursor()
    cursor.execute('SHOW SCHEMAS')
    print('✓ HMS connected')
    conn.close()
except Exception as e:
    print(f'✗ HMS failed: {e}')
"

echo "3. Testing Nessie (iceberg_nessie)..."
python3 -c "
import trino
try:
    conn = trino.dbapi.connect(host='localhost', port=8081, user='admin', catalog='iceberg_nessie', schema='default')
    cursor = conn.cursor()
    cursor.execute('SHOW SCHEMAS')
    print('✓ Nessie connected')
    conn.close()
except Exception as e:
    print(f'✗ Nessie failed: {e}')
"

echo ""
echo "Running light UPDATE stress test..."
echo "1" | python3 stress_test_update.py
