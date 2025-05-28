import psycopg2
import time
import random
import json
from datetime import datetime
import os

# Connect to Postgres
HOST = os.getenv("POSTGRES_HOST", "localhost")
PORT = os.getenv("POSTGRES_PORT", "5433") # Local mapped port
USER = os.getenv("POSTGRES_USER", "admin")
PASS = os.getenv("POSTGRES_PASSWORD", "password123")
DB = "urban_analytics"

def get_conn():
    return psycopg2.connect(host=HOST, port=PORT, user=USER, password=PASS, dbname=DB)

def simulate_traffic():
    print(f"🚀 Starting Direct Database Traffic Simulator (Target: {HOST}:{PORT})...")
    conn = get_conn()
    cur = conn.cursor()
    
    # Ensure schema consistency on startup
    try:
        cur.execute("DROP TABLE IF EXISTS serving.vehicle_locations CASCADE;")
        cur.execute("""
            CREATE TABLE serving.vehicle_locations (
                vehicle_id VARCHAR(50) PRIMARY KEY,
                timestamp TIMESTAMP,
                latitude DOUBLE PRECISION,
                longitude DOUBLE PRECISION,
                speed DOUBLE PRECISION,
                heading DOUBLE PRECISION,
                route_id VARCHAR(50),
                processing_time TIMESTAMP,
                geom GEOMETRY(POINT, 4326)
            );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_vehicle_locations_geom ON serving.vehicle_locations USING GIST (geom);")
        conn.commit()
        print("✅ Reset 'serving.vehicle_locations' table schema with PostGIS Geometry.")
    except Exception as e:
        print(f"⚠️ Warning during table reset: {e}")
        conn.rollback()

    vehicles = []
    cities = {
        "London":     {"lat": 51.5074, "lon": -0.1278},
        "New York":   {"lat": 40.7128, "lon": -74.0060},
        "Tokyo":      {"lat": 35.6762, "lon": 139.6503},
        "Paris":      {"lat": 48.8566, "lon": 2.3522},
        "Sydney":     {"lat": -33.8688, "lon": 151.2093},
        "Cairo":      {"lat": 30.0444, "lon": 31.2357},
        "Sao Paulo":  {"lat": -23.5505, "lon": -46.6333},
        "Mumbai":     {"lat": 19.0760, "lon": 72.8777},
        "Beijing":    {"lat": 39.9042, "lon": 116.4074},
        "Dubai":      {"lat": 25.2048, "lon": 55.2708},
        "Los Angeles":{"lat": 34.0522, "lon": -118.2437},
        "Berlin":     {"lat": 52.5200, "lon": 13.4050},
        "Singapore":  {"lat": 1.3521, "lon": 103.8198},
        "Toronto":    {"lat": 43.6532, "lon": -79.3832},
        "Moscow":     {"lat": 55.7558, "lon": 37.6173}
    }
    
    # Assign each vehicle to a specific city
    vehicle_map = {}
    print("Generating 5000 vehicles...")
    for i in range(100, 5100): # 5000 vehicles
        vid = f"v-{i}"
        city_name = random.choice(list(cities.keys()))
        vehicle_map[vid] = cities[city_name]
        vehicles.append(vid)

    query = """
    INSERT INTO serving.vehicle_locations 
    (vehicle_id, timestamp, latitude, longitude, speed, heading, route_id, processing_time, geom)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326))
    ON CONFLICT (vehicle_id) DO UPDATE SET
        timestamp = EXCLUDED.timestamp,
        latitude = EXCLUDED.latitude,
        longitude = EXCLUDED.longitude,
        speed = EXCLUDED.speed,
        heading = EXCLUDED.heading,
        processing_time = EXCLUDED.processing_time,
        geom = EXCLUDED.geom;
    """

    try:
        while True:
            timestamp = datetime.now()
            batch_data = []
            
            for vid in vehicles:
                city = vehicle_map[vid]
                
                # Randomize movement around the assigned city center
                lat = city["lat"] + random.uniform(-0.05, 0.05)
                lon = city["lon"] + random.uniform(-0.05, 0.05)
                
                speed = random.uniform(10, 120)
                heading = random.uniform(0, 360)
                route = f"route_{random.randint(100, 999)}"
                
                # Note the order for MakePoint is (lon, lat)
                batch_data.append((vid, timestamp, lat, lon, speed, heading, route, timestamp, lon, lat))
            
            # Batch insert for performance
            cur.executemany(query, batch_data)
            conn.commit()
            
            print(f"✅ Updated {len(vehicles)} vehicles across {len(cities)} cities at {timestamp.strftime('%H:%M:%S')}")
            time.sleep(2) # Update every 2 seconds to be kind to CPU
            
            conn.commit()
            print(f"✅ Updated {len(vehicles)} vehicles at {timestamp.strftime('%H:%M:%S')}")
            time.sleep(1) # Fast updates
            
    except KeyboardInterrupt:
        print("\nStopping simulator...")
    finally:
        conn.close()

if __name__ == "__main__":
    simulate_traffic()
