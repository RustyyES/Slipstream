#!/bin/bash

# Ensure spark-sql-kafka package is available
# Note: In a real environment, you might mount the jar or install it.
# For local dev with internet, --packages works.

echo "Submitting Spark Job..."

# Ensure dependencies are installed in the container
docker exec -u root urban-spark-master pip install psycopg2-binary
docker exec -u root urban-spark-worker pip install psycopg2-binary

docker exec -u root urban-spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0 \
    --name "UrbanMobilityEnrichment" \
    /opt/spark/app/src/processing/stream_enrichment.py
