import os
from airflow.decorators import dag, task, task_group
from datetime import datetime, timedelta, timezone
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.state import DagRunState
from airflow.models.dagrun import DagRun
from airflow.utils.session import provide_session

from resources.business.task_group_loading_layer import loading_layer
from resources.business.fact.task_group_cleaned_layer import clean_layer
from resources.business.fact.task_group_edw_layer import edw_layer
from lib.utils import load_db_env

HOME = os.getenv('AIRFLOW_HOME')

db_env = load_db_env()
_gcp_conn_id = db_env.get('gcp_conn_id')
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

        trigger_fetch_weather_dag = TriggerDagRunOperator(
            task_id="trigger_fetch_weather_dag",
            trigger_dag_id="fetch_weather_data",
            wait_for_completion=True
        )

        @task(provide_context=True, retries=10, retry_delay=timedelta(seconds=60))
        @provide_session
        def wait_for_dag_weather(session=None, **context):
            current_dt = context["execution_date"]

            dagrun = (
                session.query(DagRun)
                .filter(
                    DagRun.dag_id == 'dag_weather',
                    DagRun.execution_date > current_dt,
                    DagRun.state == DagRunState.SUCCESS,
                )
                .order_by(DagRun.execution_date.asc())
                .first()
            )

            if dagrun:
                print(f"✅ Found DAG run: {dagrun.run_id} at {dagrun.execution_date}")
            else:
                raise ValueError("DAG dag_weather doesn't run successfully")

        loading_layer_task_group = loading_layer(**kwargs)

        clean_layer_task_group = clean_layer(**kwargs)

        edw_layer_task_group = edw_layer(**kwargs)

        loading_layer_task_group >> clean_layer_task_group >> trigger_fetch_weather_dag >> wait_for_dag_weather() >> edw_layer_task_group

    get_dag()

_table_name = 'transactions'
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
    'prefix_name': f'raw/{_table_name}',

    'developer_email': ['waooooo909@gmail.com']
}

globals()[_dag_id] = create_dag(_dag_id, _schedule, **config)