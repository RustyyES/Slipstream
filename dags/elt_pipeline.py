from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import sys
import os

# Add src to sys.path to allow imports if PYTHONPATH env var in docker-compose didn't propagation perfectly to the Python runtime inside the operator (though it should).
sys.path.append('/opt/airflow/src')

try:
    from processing.elt_tasks import process_weather_data
except ImportError as e:
    print(f"Failed to import processing.elt_tasks: {e}")
    # Fallback to avoid DAG import error
    def process_weather_data(**kwargs):
        print("Import failed, cannot execute.")

default_args = {
    'owner': 'urban-analytics',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'elt_pipeline',
    default_args=default_args,
    description='ELT Processing from MinIO to Analytical Storage',
    schedule_interval=timedelta(hours=1),
    start_date=days_ago(1),
    tags=['elt', 'duckdb', 'batch'],
    catchup=False
)

t1 = PythonOperator(
    task_id='process_weather_batch',
    python_callable=process_weather_data,
    dag=dag,
)
