from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
import os

def get_sql_path():
    # Adjust path as needed for your Airflow deployment
    return os.path.join(os.path.dirname(__file__), 'cleaning_job.sql')

with DAG(
    'silver_layer_cleaning',
    default_args={
        'owner': 'zoo-data-team',
        'retries': 1,
        'retry_delay': timedelta(minutes=10),
    },
    description='Scheduled cleaning/validation for silver layer tables',
    schedule_interval='@hourly',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['silver', 'cleaning', 'validation'],
) as dag:

    clean_silver = PostgresOperator(
        task_id='run_silver_cleaning_sql',
        postgres_conn_id='zoo_postgres',
        sql=get_sql_path(),
        autocommit=True,
    ) 