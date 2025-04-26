from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
sys.path.append("/opt/airflow/scripts")

from quality_checker import run_checks

default_args = {
    'start_date': datetime(2024, 4, 24),
    'retries': 1
}

with DAG(
    'daily_data_quality_check',
    default_args=default_args,
    schedule_interval='@daily',  # Run daily
    catchup=False
) as dag:

    run_quality_check = PythonOperator(
        task_id='run_data_quality_script',
        python_callable=run_checks
    )
