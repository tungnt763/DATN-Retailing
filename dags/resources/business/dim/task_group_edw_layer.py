from airflow.decorators import task, task_group
from lib.utils import get_edw_expressions_for_table
from resources.python_task.get_max_timestamp_task import get_max_timestamp_task
from resources.python_task.insert_job_task import insert_job_task
from resources.python_task.data_quality_check import data_quality_check
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

import os

@task_group(group_id='edw_layer')
def edw_layer(**kwargs):
    _table_name = kwargs.get('table_name')
    _project_name = kwargs.get('project')
    _input_dataset = kwargs.get('clean_dataset')
    _output_dataset = kwargs.get('dwh_dataset')

    _gcp_conn_id = kwargs.get('gcp_conn_id')

    params = get_edw_expressions_for_table(_table_name, 'edw_layer_table_info', _input_dataset, _output_dataset, _project_name)

    _sql_template = os.path.join("resources", "sql_template", _output_dataset, "dim", f"{params['scd_type']}_insert_cleaned_to_edw.sql")

    insert_cleaned_data_to_edw_table = BigQueryInsertJobOperator(
        task_id=f'insert_cleaned_data_to_edw_{_table_name}',
        configuration={
            "query": {
                "query": "{% include '" + _sql_template + "' %}",
                "useLegacySql": False,
            }
        },
        params=params,
        location='US', 
        gcp_conn_id=_gcp_conn_id
    )

    max_timestamp = get_max_timestamp_task(gcp_conn_id=_gcp_conn_id, dataset_name=_output_dataset, table_name=_table_name)

    dqc = data_quality_check.override(task_id=f'data_quality_check_{_table_name}')(_table_name, _output_dataset)

    insert_job = insert_job_task.override(task_id=f'insert_edw_{_table_name}_job_to_log')(gcp_conn_id=_gcp_conn_id, dataset_name=_output_dataset, table_name=_table_name)

    max_timestamp >> insert_cleaned_data_to_edw_table >> dqc >> insert_job