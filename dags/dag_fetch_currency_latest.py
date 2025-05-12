from airflow.decorators import dag, task
from resources.python_task.fetch_currency_rates import fetch_currency_rates
from datetime import datetime, timedelta
import os

default_args = {
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='fetch_currency_latest',
    default_args=default_args,
    schedule_interval='0 * * * *',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['fetch', 'currency'],
)
def fetch_currency_latest():
    
    fetch_currency_latest_task = fetch_currency_rates(
        output_path=os.path.join(os.path.dirname("AIRFLOW_HOME"), "include", "dataset", "currency.csv")
    )

    fetch_currency_latest_task

dag_fetch_currency_latest = fetch_currency_latest()