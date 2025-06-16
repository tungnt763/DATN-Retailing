import os
from airflow.decorators import dag, task, task_group
from datetime import datetime, timedelta, timezone
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from resources.business.task_group_loading_layer import loading_layer
from resources.business.dim.task_group_cleaned_layer import clean_layer
from resources.business.dim.task_group_edw_layer import edw_layer
from lib.utils import load_db_env, get_table_names
from resources.python_task.fetch_location_data import fetch_location_data

HOME = os.getenv('AIRFLOW_HOME')

db_env = load_db_env()
_gcp_conn_id = db_env.get('gcp_conn_id')
_project = db_env.get('project')
_load_dataset = db_env.get('load_dataset')
_clean_dataset = db_env.get('clean_dataset')
_dwh_dataset = db_env.get('dwh_dataset')
_bucket_name = db_env.get('bucket_name')

_table_names = get_table_names()

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
        max_active_runs=1,
        tags=[kwargs.get('table_name')]
    )
    def get_dag():

        loading_layer_task_group = loading_layer(**kwargs)

        clean_layer_task_group = clean_layer(**kwargs)

        edw_layer_task_group = edw_layer(**kwargs)

        if kwargs.get('table_name') in ['stores', 'customers']:
            loading_layer_task_group >> clean_layer_task_group >> fetch_location_data(gcp_conn_id=kwargs.get('gcp_conn_id'), project_name=kwargs.get('project'), dataset_name=kwargs.get('clean_dataset'), table_name=kwargs.get('table_name'), bucket_name=kwargs.get('bucket_name')) >> edw_layer_task_group
        else:
            loading_layer_task_group >> clean_layer_task_group >> edw_layer_task_group


    get_dag()

for _table_name in _table_names:
    if _table_name == 'transactions':
        continue

    _dag_id = f'dag_{_table_name}'
    _schedule = None

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






