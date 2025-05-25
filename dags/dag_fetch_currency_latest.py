from airflow.decorators import dag, task
from resources.python_task.fetch_currency_rates import fetch_currency_rates
from datetime import datetime, timedelta
from lib.utils import load_db_env

db_env = load_db_env()
_gcp_conn_id = db_env.get('gcp_conn_id')
_bucket_name = db_env.get('bucket_name')

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
    schedule_interval='0 0 * * *',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['fetch', 'currency'],
)
def fetch_currency_latest():
    
    fetch_currency_rates(
        gcp_conn_id=_gcp_conn_id,
        bucket_name=_bucket_name,
        table_name='currency'
    )

fetch_currency_latest()