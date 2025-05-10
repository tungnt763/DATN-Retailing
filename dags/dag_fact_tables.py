import os
from airflow.decorators import dag, task, task_group
from datetime import datetime, timedelta, timezone
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from resources.business.task_group_loading_layer import loading_layer
# from resources.business.task_group_cleaned_layer import clean_layer
# from resources.business.task_group_edw_layer import edw_layer
from lib.utils import load_db_env

HOME = os.getenv('AIRFLOW_HOME')

db_env = load_db_env()
_gcp_conn_id = 'gcp'
_project = db_env.get('project')
_load_dataset = db_env.get('load_dataset')
_clean_dataset = db_env.get('clean_dataset')
_dwh_dataset = db_env.get('dwh_dataset')
_bucket_name = db_env.get('bucket_name')

default_args = {
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

def create_dag(_dag_id, _schedule, **kwargs):

    @dag(
        dag_id=_dag_id,
        schedule_interval=_schedule,
        start_date=datetime(2023, 1, 1),
        catchup=False,
        default_args=default_args,
        tags=[kwargs.get('table_name')]
    )
    def get_dag():

        kwargs['loaded_batch'] = '{{ execution_date.int_timestamp }}'
        
        check_gcs_file = GCSObjectsWithPrefixExistenceSensor(
            task_id=f'check_gcs_{kwargs.get("table_name")}_file',
            bucket=kwargs.get('bucket_name'),
            prefix=kwargs.get('prefix_name'), 
            google_cloud_conn_id=kwargs.get('gcp_conn_id'),
            timeout=300,
            poke_interval=60,
            soft_fail=True,
            mode='reschedule',
        )

        loading_layer_task_group = loading_layer(**kwargs)

        # clean_layer_task_group = clean_layer(**kwargs)

        # edw_layer_task_group = edw_layer(**kwargs)

        check_gcs_file >> loading_layer_task_group # >> clean_layer_task_group >> edw_layer_task_group 

    get_dag()

_table_name = 'transactions'
_dag_id = f'dag_{_table_name}'
_schedule = '0 0 * * *'

config = {
    'dag_id': _dag_id,
    'schedule': _schedule,

    'gcp_conn_id': _gcp_conn_id,
    'project': _project,
    'load_dataset': _load_dataset,
    'clean_dataset': _clean_dataset,
    'dwh_dataset': _dwh_dataset,

    'bucket_name': _bucket_name,
    'table_name': _table_name,
    'prefix_name': f'raw/{_table_name}'
}

globals()[_dag_id] = create_dag(_dag_id, _schedule, **config)