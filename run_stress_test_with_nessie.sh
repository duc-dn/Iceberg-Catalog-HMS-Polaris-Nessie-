#!/bin/bash

echo "DOCKER COMPOSE STARTUP AND NESSIE STRESS TEST"
echo "=============================================="

# Start Docker Compose
echo "Starting Docker Compose services..."
docker-compose -f docker-compose-final.yml up -d

# Wait for services to be ready
echo "Waiting for services to start..."
sleep 30

# Check service status
echo "Checking service status..."
echo "Polaris: $(curl -s -o /dev/null -w "%{http_code}" http://localhost:8181/api/v1/config)"
echo "Trino: $(curl -s -o /dev/null -w "%{http_code}" http://localhost:8081/v1/info)"
echo "Nessie: $(curl -s -o /dev/null -w "%{http_code}" http://localhost:19120/api/v2/config)"

# Test Nessie connectivity first
echo "Testing Nessie catalog connectivity..."
python3 test_nessie_catalog.py

if [ $? -eq 0 ]; then
    echo "✓ Nessie test passed! Starting stress test..."
    
    # Run comprehensive stress test
    python3 stress_test_comprehensive.py
else
    echo "✗ Nessie test failed. Running basic stress test without Nessie..."
    python3 stress_test_insert.py
fi
