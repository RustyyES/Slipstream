import duckdb
import os

def process_weather_data(**kwargs):
    """
    Reads weather JSON from MinIO, aggregates it using DuckDB.
    """
    print("Initializing DuckDB with S3 support...")
    con = duckdb.connect()
    
    # Install/Load httpfs extension for S3 access
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")
    
    # Configure S3 connectivity
    con.execute("SET s3_endpoint='minio:9000';")
    con.execute("SET s3_use_ssl=false;")
    con.execute("SET s3_url_style='path';")
    con.execute("SET s3_access_key_id='admin';")
    con.execute("SET s3_secret_access_key='password123';")
    
    print("Querying MinIO raw-data/weather bucket...")
    
    try:
        # Check if files exist first or handle error gracefully
        # DuckDB 0.9+ supports list_files or globs.
        # If no files, it might error.
        
        query = """
            SELECT 
                city,
                AVG(temperature) as avg_temp,
                MAX(temperature) as max_temp,
                MIN(temperature) as min_temp,
                AVG(humidity) as avg_humidity,
                COUNT(*) as record_count,
                MAX(timestamp) as last_update
            FROM read_json_auto('s3://raw-data/weather/*.json', ignore_errors=true)
            GROUP BY city
        """
        
        df = con.execute(query).fetchdf()
        
        print(f"Weather Aggregation Results ({len(df)} rows):")
        print(df)
        
        # Example: Write valid results to 'processed-data' bucket in Parquet
        if not df.empty:
            # Write to MinIO (Data Lake - Processed)
            con.execute("""
                COPY (
                    SELECT 
                        city,
                        AVG(temperature) as avg_temp,
                        MAX(timestamp) as last_update
                    FROM read_json_auto('s3://raw-data/weather/*.json', ignore_errors=true)
                    GROUP BY city
                ) TO 's3://processed-data/weather_summary.parquet' (FORMAT PARQUET);
            """)
            print("Written summary to s3://processed-data/weather_summary.parquet")

            # Write to Postgres (Serving Layer)
            import psycopg2
            pg_conn = psycopg2.connect(
                dbname=os.getenv("POSTGRES_DB", "urban_analytics"),
                user=os.getenv("POSTGRES_USER", "admin"),
                password=os.getenv("POSTGRES_PASSWORD", "password123"),
                host="urban-postgres",
                port=os.getenv("POSTGRES_PORT", "5432")
            )
            pg_cur = pg_conn.cursor()
            
            # Ensure table exists (idempotent)
            pg_cur.execute("""
                CREATE SCHEMA IF NOT EXISTS serving;
                CREATE TABLE IF NOT EXISTS serving.weather_summary (
                    city VARCHAR(50) PRIMARY KEY,
                    avg_temp DOUBLE PRECISION,
                    max_temp DOUBLE PRECISION,
                    min_temp DOUBLE PRECISION,
                    avg_humidity DOUBLE PRECISION,
                    record_count INT,
                    last_update TIMESTAMP
                );
            """)
            
            # Upsert logic
            insert_query = """
                INSERT INTO serving.weather_summary (city, avg_temp, max_temp, min_temp, avg_humidity, record_count, last_update)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (city) DO UPDATE SET
                    avg_temp = EXCLUDED.avg_temp,
                    max_temp = EXCLUDED.max_temp,
                    min_temp = EXCLUDED.min_temp,
                    avg_humidity = EXCLUDED.avg_humidity,
                    record_count = EXCLUDED.record_count,
                    last_update = EXCLUDED.last_update;
            """
            
            # Convert DF to list of tuples for insertion
            # Note: df is a pandas DataFrame returned by fetchdf()
            records = df.to_records(index=False).tolist()
            
            for row in records:
                pg_cur.execute(insert_query, row)
            
            pg_conn.commit()
            pg_cur.close()
            pg_conn.close()
            print("Successfully upset aggregated weather data to Postgres serving layer.")

    except Exception as e:
        print(f"Error executing DuckDB query: {e}")
        # It's possible no files exist yet
        if "No files found" in str(e):
            print("No weather data found to process.")
        else:
            raise e
