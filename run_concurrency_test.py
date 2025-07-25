#!/usr/bin/env python3
"""
Simple runner script for concurrency tests
"""

import sys
import os
import subprocess
import time

def check_trino_connection():
    """Check if Trino is accessible"""
    try:
        import trino
        conn = trino.dbapi.connect(
            host='localhost',
            port=8081,
            user='admin'
        )
        cursor = conn.cursor()
        cursor.execute("SHOW CATALOGS")
        catalogs = [row[0] for row in cursor.fetchall()]
        conn.close()
        
        print(f"Available catalogs: {catalogs}")
        
        required_catalogs = ['iceberg', 'iceberg_hms']
        missing_catalogs = [cat for cat in required_catalogs if cat not in catalogs]
        
        if missing_catalogs:
            print(f"Missing required catalogs: {missing_catalogs}")
            return False
        
        return True
    except Exception as e:
        print(f"Failed to connect to Trino: {e}")
        return False

def install_requirements():
    """Install required packages"""
    print("Installing requirements...")
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])
        print("Requirements installed successfully!")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Failed to install requirements: {e}")
        return False

def run_test_scenario(scenario="all"):
    """Run specific test scenario"""
    print(f"Running test scenario: {scenario}")
    
    if scenario == "all":
        print("Running all concurrency tests...")
        subprocess.run([sys.executable, "test_concurrency.py"])
    elif scenario == "quick":
        print("Running quick test (reduced load)...")
        # You can modify the test script to accept parameters
        subprocess.run([sys.executable, "test_concurrency.py", "--quick"])
    elif scenario == "stress":
        print("Running stress test (increased load)...")
        subprocess.run([sys.executable, "test_concurrency.py", "--stress"])
    else:
        print(f"Unknown scenario: {scenario}")

def main():
    print("=== Trino Concurrency Test Runner ===")
    
    # Check if requirements are installed
    try:
        import trino
        print("✓ Trino package is available")
    except ImportError:
        print("✗ Trino package not found")
        if input("Install requirements? (y/n): ").lower() == 'y':
            if not install_requirements():
                sys.exit(1)
        else:
            sys.exit(1)
    
    # Check Trino connection
    print("\nChecking Trino connection...")
    if not check_trino_connection():
        print("✗ Cannot connect to Trino or missing catalogs")
        print("Make sure Docker services are running:")
        print("  docker compose -f docker-compose-final.yml up -d")
        sys.exit(1)
    
    print("✓ Trino connection successful")
    
    # Run tests
    print("\nSelect test scenario:")
    print("1. All tests (default)")
    print("2. Quick test")
    print("3. Stress test")
    
    choice = input("Enter choice (1-3) or press Enter for default: ").strip()
    
    scenario_map = {
        "1": "all",
        "2": "quick", 
        "3": "stress",
        "": "all"
    }
    
    scenario = scenario_map.get(choice, "all")
    
    print(f"\nStarting tests in 3 seconds...")
    time.sleep(3)
    
    run_test_scenario(scenario)

if __name__ == "__main__":
    main()
