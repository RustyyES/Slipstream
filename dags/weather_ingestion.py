from datetime import datetime, timedelta
import json
import random
import boto3
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'urban-analytics',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'weather_ingestion',
    default_args=default_args,
    description='Ingest mock weather data to MinIO',
    schedule_interval=timedelta(hours=1),
    start_date=days_ago(1),
    tags=['ingestion', 'weather'],
    catchup=False
)

def generate_and_upload_weather(**kwargs):
    """
    Generates mock weather data and uploads directly to MinIO using boto3.
    """
    timestamp = datetime.now().isoformat()
    data = {
        "city": "London",
        "timestamp": timestamp,
        "temperature": round(random.uniform(5.0, 25.0), 2),
        "humidity": random.randint(40, 90),
        "condition": random.choice(["Sunny", "Cloudy", "Rain", "Windy"]),
        "wind_speed": round(random.uniform(0, 30), 2)
    }
    
    # Use boto3 to upload directly to MinIO
    s3 = boto3.client(
        's3',
        endpoint_url='http://minio:9000',
        aws_access_key_id='admin',
        aws_secret_access_key='password123'
    )
    
    # Create unique filename
    file_name = f"weather/london_{timestamp.replace(':', '-')}.json"
    
    s3.put_object(
        Body=json.dumps(data),
        Bucket='raw-data',
        Key=file_name
    )
    
    print(f"Uploaded weather data to MinIO: s3://raw-data/{file_name}")
    print(f"Data: {data}")
    return data

t1 = PythonOperator(
    task_id='generate_and_upload_weather',
    python_callable=generate_and_upload_weather,
    dag=dag,
)
