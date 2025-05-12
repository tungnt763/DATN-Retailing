from airflow.decorators import task, task_group
from resources.python_task.get_max_timestamp_task import get_max_timestamp_task
from resources.python_task.insert_job_task import insert_job_task
from resources.python_task.data_quality_check import data_quality_check
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from lib.utils import get_clean_expressions_for_table

import os

@task_group(group_id='clean_layer')
def clean_layer(**kwargs):
    _table_name = kwargs.get('table_name')
    _project_name = kwargs.get('project')
    _input_dataset = kwargs.get('load_dataset')
    _output_dataset = kwargs.get('clean_dataset')

    _gcp_conn_id = kwargs.get('gcp_conn_id')

    _sql_template = os.path.join("resources", "sql_template", _output_dataset, "dim", "insert_cleaned_transformed_data_to_table.sql")

    params = get_clean_expressions_for_table(_table_name, 'clean_layer_table_info', _input_dataset, _output_dataset, _project_name)

    insert_cleaned_data_to_table = BigQueryInsertJobOperator(
        task_id=f'insert_cleaned_data_to_{_table_name}',
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

    insert_job = insert_job_task.override(task_id=f'insert_cleaned_{_table_name}_job_to_log')(gcp_conn_id=_gcp_conn_id, dataset_name=_output_dataset, table_name=_table_name)

    max_timestamp >> insert_cleaned_data_to_table >> dqc >> insert_job