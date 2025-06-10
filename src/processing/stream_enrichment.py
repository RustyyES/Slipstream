
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, struct, to_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType
import os
import psycopg2
import psycopg2.extras

# Configuration
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:29092')
TOPIC = 'gps-events'
POSTGRES_DB = "urban_analytics"
POSTGRES_USER = "admin"
POSTGRES_PASSWORD = "password123"
POSTGRES_HOST = "postgres"
POSTGRES_PORT = "5432"

def main():
    spark = SparkSession.builder \
        .appName("UrbanMobilityStreaming") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")

    # Define Schema for JSON Data
    schema = StructType([
        StructField("vehicle_id", StringType(), True),
        StructField("timestamp", StringType(), True), # ISO string
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("speed", DoubleType(), True),
        StructField("heading", DoubleType(), True),
        StructField("route_id", StringType(), True)
    ])

    # Read from Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 10000) \
        .load()

    # Parse JSON
    json_df = df.select(
        from_json(col("value").cast("string"), schema).alias("data"),
        col("timestamp").alias("kafka_timestamp")
    ).select("data.*", "kafka_timestamp").filter(col("vehicle_id").isNotNull())
    
    # Transformation (Simple Enrichment example)
    enriched_df = json_df.withColumn("processing_time", current_timestamp())

    # Write to MinIO (Parquet) - Raw Layer using foreachBatch for reliability
    checkpoint_location_minio = "s3a://checkpoints/gps/minio"
    
    def write_to_minio(batch_df, epoch_id):
        count = batch_df.count()
        if count == 0:
            return
        try:
            # Write batch as parquet to MinIO
            output_path = "s3a://raw-data/gps/"
            batch_df.write.mode("append").parquet(output_path)
            print(f"DEBUG: Successfully wrote batch {epoch_id} ({count} records) to MinIO")
        except Exception as e:
            print(f"ERROR: Failed to write batch {epoch_id} to MinIO: {e}")
    
    try:
        query_minio = enriched_df.writeStream \
            .foreachBatch(write_to_minio) \
            .option("checkpointLocation", checkpoint_location_minio) \
            .trigger(processingTime="10 seconds") \
            .start()
        print(f"DEBUG: MinIO stream started successfully: {query_minio.id}")
    except Exception as e:
        print(f"ERROR: Failed to start MinIO stream: {e}")
        query_minio = None

    # Write to JDBC (Serving Layer)
    def foreach_batch_function(batch_df, epoch_id):
        # Debug: Print batch size
        count = batch_df.count()
        if count == 0:
            return
            
        print(f"DEBUG: Processing Batch {epoch_id} with {count} records")
        
        # Collect data to driver for insertion
        # Note: For very large batches, bulk copy or parallel connections from executors is better.
        # But for 5000 records, driver collection is acceptable and simpler for robust UPSERT.
        rows = batch_df.collect()
        
        try:
            conn = psycopg2.connect(
                dbname=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
                host=POSTGRES_HOST,
                port=POSTGRES_PORT
            )
            cur = conn.cursor()
            
            insert_query = """
                INSERT INTO serving.vehicle_locations 
                (vehicle_id, route_id, latitude, longitude, speed, heading, timestamp, location, last_updated)
                VALUES (%s, %s, %s, %s, %s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326), NOW());
            """
            
            data_tuples = [
                (
                    row.vehicle_id, 
                    row.route_id, 
                    row.latitude, 
                    row.longitude, 
                    row.speed, 
                    row.heading, 
                    row.timestamp,
                    row.longitude, # for PostGIS MakePoint (lon, lat)
                    row.latitude
                ) for row in rows
            ]
            
            psycopg2.extras.execute_batch(cur, insert_query, data_tuples)
            
            conn.commit()
            cur.close()
            conn.close()
            print("Successfully processed batch to Postgres")
            
        except Exception as e:
            print(f"ERROR: Failed to write batch {epoch_id} to Postgres: {e}")
            # Don't raise error to keep stream alive, but log strictly.
            # in production you might want to DLQ or retry
            pass

    checkpoint_location_postgres = "s3a://checkpoints/gps/postgres"
    query_jdbc = enriched_df.writeStream \
        .foreachBatch(foreach_batch_function) \
        .option("checkpointLocation", checkpoint_location_postgres) \
        .trigger(processingTime="5 seconds") \
        .start()
    print(f"DEBUG: Postgres stream started: {query_jdbc.id}")
    print(f"DEBUG: Active streams: {spark.streams.active}")

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
