from airflow.decorators import task, task_group
from lib.utils import get_edw_expressions_for_fact_table
from resources.python_task.get_max_timestamp_task import get_max_timestamp_task
from resources.python_task.insert_job_task import insert_job_task
from resources.python_task.data_quality_check import data_quality_check
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import timedelta

import os

@task_group(group_id='edw_layer')
def edw_layer(**kwargs):
    _table_name = kwargs.get('table_name')
    _project_name = kwargs.get('project')
    _input_dataset = kwargs.get('clean_dataset')
    _output_dataset = kwargs.get('dwh_dataset')

    _gcp_conn_id = kwargs.get('gcp_conn_id')

    params = get_edw_expressions_for_fact_table(_table_name, 'edw_layer_table_info', _input_dataset, _output_dataset, _project_name)

    _sql_template_create_temp_table = os.path.join("resources", "sql_template", _output_dataset, "fact", "create_temp_table.sql")
    _sql_template_insert_clean_table_to_temp = os.path.join("resources", "sql_template", _output_dataset, "fact", "insert_clean_table_to_temp.sql")
    _sql_template_insert_fact_table_to_temp = os.path.join("resources", "sql_template", _output_dataset, "fact", "insert_fact_table_to_temp.sql")
    _sql_template_insert_temp_table_to_fact = os.path.join("resources", "sql_template", _output_dataset, "fact", "insert_temp_table_to_fact.sql")
    _sql_template_drop_temp_table = os.path.join("resources", "sql_template", _output_dataset, "fact", "drop_temp_table.sql")

    create_temp_table_task = BigQueryInsertJobOperator(
        task_id=f'create_temp_table_{_table_name}',
        configuration={
            "query": {
                "query": "{% include '" + _sql_template_create_temp_table + "' %}",
                "useLegacySql": False,
            }
        },
        params=params,
        location='US', 
        gcp_conn_id=_gcp_conn_id,
        retries=2,
        retry_delay=timedelta(minutes=5)
    )

    insert_clean_table_to_temp_task = BigQueryInsertJobOperator(
        task_id=f'insert_clean_table_to_temp_{_table_name}',
        configuration={
            "query": {
                "query": "{% include '" + _sql_template_insert_clean_table_to_temp + "' %}",
                "useLegacySql": False,
            }
        },
        params=params,
        location='US', 
        gcp_conn_id=_gcp_conn_id,
        retries=2,
        retry_delay=timedelta(minutes=5)
    )

    insert_fact_table_to_temp_task = BigQueryInsertJobOperator(
        task_id=f'insert_fact_table_to_temp_{_table_name}',
        configuration={
            "query": {
                "query": "{% include '" + _sql_template_insert_fact_table_to_temp + "' %}",
                "useLegacySql": False,
            }
        },
        params=params,
        location='US', 
        gcp_conn_id=_gcp_conn_id,
        retries=2,
        retry_delay=timedelta(minutes=5)
    )

    insert_temp_table_to_fact_task = BigQueryInsertJobOperator(
        task_id=f'insert_temp_table_to_fact_{_table_name}',
        configuration={
            "query": {
                "query": "{% include '" + _sql_template_insert_temp_table_to_fact + "' %}",
                "useLegacySql": False,
            }
        },
        params=params,
        location='US', 
        gcp_conn_id=_gcp_conn_id,
        retries=2,
        retry_delay=timedelta(minutes=5)
    )

    drop_temp_table_task = BigQueryInsertJobOperator(
        task_id=f'drop_temp_table_{_table_name}',
        configuration={
            "query": {
                "query": "{% include '" + _sql_template_drop_temp_table + "' %}",
                "useLegacySql": False,
            }
        },
        params=params,
        location='US', 
        gcp_conn_id=_gcp_conn_id,
        retries=2,
        retry_delay=timedelta(minutes=5)
    )

    max_timestamp = get_max_timestamp_task(gcp_conn_id=_gcp_conn_id, dataset_name=_output_dataset, table_name=_table_name)

    dqc = data_quality_check.override(task_id=f'data_quality_check_{_table_name}')(_table_name, _output_dataset)

    insert_job = insert_job_task.override(task_id=f'insert_edw_{_table_name}_job_to_log')(gcp_conn_id=_gcp_conn_id, dataset_name=_output_dataset, table_name=_table_name)
    
    max_timestamp >> create_temp_table_task >> insert_clean_table_to_temp_task >> insert_fact_table_to_temp_task >> insert_temp_table_to_fact_task >> [dqc, drop_temp_table_task] >> insert_job