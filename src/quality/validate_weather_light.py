import psycopg2
import os
import sys
import socket

def is_running_in_docker():
    try:
        socket.gethostbyname("urban-postgres")
        return True
    except socket.error:
        return False

def validate_weather_light():
    print("Starting Lightweight Weather Validation...")
    
    # Determine Host/Port
    if is_running_in_docker():
        host = "urban-postgres"
        port = "5432"
    else:
        host = "localhost"
        port = "5433"
        
    conn_params = {
        "dbname": "urban_analytics",
        "user": "admin",
        "password": "password123",
        "host": host,
        "port": port
    }
    
    try:
        print(f"Connecting to Postgres at {host}:{port}...")
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()
    except Exception as e:
        print(f"❌ Connection Failed: {e}")
        sys.exit(1)

    # Define Expectations as SQL Queries
    expectations = [
        {
            "description": "City column should not contain NULLs",
            "query": "SELECT COUNT(*) FROM serving.weather_summary WHERE city IS NULL",
            "expected_value": 0
        },
        {
            "description": "Temperature must be between -50 and 60",
            "query": "SELECT COUNT(*) FROM serving.weather_summary WHERE avg_temp < -50 OR avg_temp > 60",
            "expected_value": 0
        },
        {
            "description": "Humidity must be between 0 and 100",
            "query": "SELECT COUNT(*) FROM serving.weather_summary WHERE avg_humidity < 0 OR avg_humidity > 100",
            "expected_value": 0
        },
        {
            "description": "Record count should be positive",
            "query": "SELECT COUNT(*) FROM serving.weather_summary WHERE record_count <= 0",
            "expected_value": 0
        }
    ]
    
    failures = 0
    print("\nRunning Expectations...")
    
    for exp in expectations:
        try:
            cur.execute(exp["query"])
            result = cur.fetchone()[0]
            
            if result == exp["expected_value"]:
                print(f"✅ PASS: {exp['description']}")
            else:
                print(f"❌ FAIL: {exp['description']} (Found {result} violations)")
                failures += 1
        except Exception as e:
            print(f"⚠️ ERROR executing check '{exp['description']}': {e}")
            failures += 1
            
    conn.close()
    
    if failures == 0:
        print("\n🎉 ALL VALIDATION CHECKS PASSED")
        sys.exit(0)
    else:
        print(f"\n❌ VALIDATION FAILED with {failures} errors")
        sys.exit(1)

if __name__ == "__main__":
    validate_weather_light()
