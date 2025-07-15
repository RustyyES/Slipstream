from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
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
    'process_daily_aggregates',
    default_args=default_args,
    description='Calculate daily traffic and user metrics',
    schedule_interval='0 1 * * *', # Daily at 1 AM
    start_date=days_ago(1),
    tags=['batch', 'analytics', 'daily'],
    catchup=False
)

# SQL for creating/refreshing the daily summary materialized view or table
# Using PostgresOperator for direct database push-down
create_daily_summary_sql = """
    CREATE SCHEMA IF NOT EXISTS analytics;
    
    CREATE TABLE IF NOT EXISTS analytics.daily_traffic_stats (
        date DATE PRIMARY KEY,
        total_trips INT,
        avg_speed DOUBLE PRECISION,
        active_vehicles INT,
        total_distance_km DOUBLE PRECISION
    );
    
    INSERT INTO analytics.daily_traffic_stats (date, total_trips, avg_speed, active_vehicles, total_distance_km)
    SELECT
        DATE(timestamp) as date,
        COUNT(*) as total_trips,
        AVG(speed) as avg_speed,
        COUNT(DISTINCT vehicle_id) as active_vehicles,
        SUM(speed * 0.0027) as total_distance_km -- Rough approximation (speed m/s * interval) / 1000
    FROM serving.vehicle_locations
    WHERE timestamp >= CURRENT_DATE - INTERVAL '1 day'
      AND timestamp < CURRENT_DATE
    GROUP BY DATE(timestamp)
    ON CONFLICT (date) DO UPDATE SET
        total_trips = EXCLUDED.total_trips,
        avg_speed = EXCLUDED.avg_speed,
        active_vehicles = EXCLUDED.active_vehicles,
        total_distance_km = EXCLUDED.total_distance_km;
"""

t1 = PostgresOperator(
    task_id='calculate_daily_stats',
    postgres_conn_id='postgres_default', # Requires connection 'postgres_default' to be set to urban-postgres
    sql=create_daily_summary_sql,
    dag=dag,
)
