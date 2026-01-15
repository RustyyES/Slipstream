import asyncio
import os
import asyncpg

# Configuration
POSTGRES_USER = os.getenv("POSTGRES_USER", "admin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password123")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5433") # Default to dev port
POSTGRES_DB = os.getenv("POSTGRES_DB", "urban_analytics")

DATABASE_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

INIT_SQL = """
CREATE SCHEMA IF NOT EXISTS serving;

-- Vehicle Locations Table (for Traffic & Geospatial API)
CREATE TABLE IF NOT EXISTS serving.vehicle_locations (
    vehicle_id VARCHAR(50) PRIMARY KEY,
    city VARCHAR(50),
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    speed DOUBLE PRECISION,
    timestamp TIMESTAMP,
    geom GEOMETRY(POINT, 4326)
);

CREATE INDEX IF NOT EXISTS idx_vehicle_locations_geom ON serving.vehicle_locations USING GIST (geom);

-- Weather Summary Table (for Weather API)
CREATE TABLE IF NOT EXISTS serving.weather_summary (
    city VARCHAR(50) PRIMARY KEY,
    avg_temp DOUBLE PRECISION,
    condition VARCHAR(50),
    timestamp TIMESTAMP
);

-- Insert dummy data for testing
INSERT INTO serving.weather_summary (city, avg_temp, condition, timestamp)
VALUES ('London', 15.5, 'Cloudy', NOW())
ON CONFLICT (city) DO NOTHING;

INSERT INTO serving.vehicle_locations (vehicle_id, city, lat, lon, speed, timestamp, geom)
VALUES 
    ('test-v-1', 'London', 51.5074, -0.1278, 45.0, NOW(), ST_SetSRID(ST_MakePoint(-0.1278, 51.5074), 4326)),
    ('test-v-2', 'New York', 40.7128, -74.0060, 60.0, NOW(), ST_SetSRID(ST_MakePoint(-74.0060, 40.7128), 4326))
ON CONFLICT (vehicle_id) DO NOTHING;
"""

async def init_db():
    print(f"🔌 Connecting to {DATABASE_URL}...")
    try:
        conn = await asyncpg.connect(DATABASE_URL)
        print("⚡ Executing Schema Initialization...")
        await conn.execute(INIT_SQL)
        print("✅ Database Initialized Successfully!")
        await conn.close()
    except Exception as e:
        print(f"❌ Database Initialization Failed: {e}")
        exit(1)

if __name__ == "__main__":
    asyncio.run(init_db())
