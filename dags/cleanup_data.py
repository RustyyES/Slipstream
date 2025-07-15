from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import psycopg2
import os

default_args = {
    'owner': 'urban-analytics',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'cleanup_old_data',
    default_args=default_args,
    description='Archival and cleanup of data older than 30 days',
    schedule_interval='@monthly', 
    start_date=days_ago(1),
    tags=['maintenance', 'cleanup'],
    catchup=False
)

def cleanup_minio(**kwargs):
    # This would use S3Hook to list and delete objects > 30 days
    # For simulation, we just print
    print("Scanning MinIO raw-data for objects > 30 days...")
    print("Moved 0 objects to glacier/archive.")

def cleanup_postgres(**kwargs):
    conn = psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB", "urban_analytics"),
        user=os.getenv("POSTGRES_USER", "admin"),
        password=os.getenv("POSTGRES_PASSWORD", "password123"),
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", "5432")
    )
    cur = conn.cursor()
    # Delete raw locations older than 7 days (keep summary forever)
    cur.execute("DELETE FROM serving.vehicle_locations WHERE timestamp < NOW() - INTERVAL '7 days';")
    deleted = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()
    print(f"Cleaned up {deleted} old records from Postgres.")

t1 = PythonOperator(
    task_id='cleanup_minio_raw',
    python_callable=cleanup_minio,
    dag=dag,
)

t2 = PythonOperator(
    task_id='cleanup_postgres_tables',
    python_callable=cleanup_postgres,
    dag=dag,
)

t1 >> t2
