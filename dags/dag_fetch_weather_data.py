from airflow.decorators import dag, task
from resources.python_task.fetch_weather_data import fetch_and_upload_weather_to_gcs
from datetime import datetime, timedelta
from lib.utils import load_db_env

db_env = load_db_env()
_gcp_conn_id = db_env.get('gcp_conn_id')
_project = db_env.get('project')
_clean_dataset = db_env.get('clean_dataset')
_bucket_name = db_env.get('bucket_name')

default_args = {
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}
@dag(   
    default_args=default_args,
    dag_id='fetch_weather_data',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['weather', 'fetch']
)
def fetch_weather_data():

    fetch_and_upload_weather_to_gcs(
        gcp_conn_id=_gcp_conn_id,
        project_name=_project,
        dataset_name=_clean_dataset,
        bucket_name=_bucket_name,
        table_name='weather'
    )
    
fetch_weather_data()
