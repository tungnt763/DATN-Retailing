from airflow.decorators import task, task_group
from resources.python_task.get_max_timestamp_task import get_max_timestamp_task
from resources.python_task.insert_job_task import insert_job_task
from resources.python_task.data_quality_check import data_quality_check
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from lib.utils import get_clean_expressions_for_fact_table
from resources.python_task.check_invalid_rows_and_notify import check_invalid_rows_and_notify
from airflow.utils.email import send_email
from datetime import timedelta

import os

@task_group(group_id='clean_layer')
def clean_layer(**kwargs):
    _table_name = kwargs.get('table_name')
    _project_name = kwargs.get('project')
    _input_dataset = kwargs.get('load_dataset')
    _output_dataset = kwargs.get('clean_dataset')

    _gcp_conn_id = kwargs.get('gcp_conn_id')

    _developer_email = kwargs.get('developer_email')

    _sql_template_clean_transform_data = os.path.join("resources", "sql_template", _output_dataset, "fact", "clean_transform_data.sql")
    _sql_template_insert_cleaned_transformed_data_to_table = os.path.join("resources", "sql_template", _output_dataset, "fact", "insert_cleaned_transformed_data_to_table.sql")
    _sql_template_insert_invalid_data_to_table = os.path.join("resources", "sql_template", _output_dataset, "fact", "insert_invalid_data_to_table.sql")
    _sql_template_drop_temp_table = os.path.join("resources", "sql_template", _output_dataset, "fact", "drop_temp_table.sql")

    params = get_clean_expressions_for_fact_table(_table_name, 'clean_layer_table_info', _input_dataset, _output_dataset, _project_name)

    clean_transform_data_task = BigQueryInsertJobOperator(
        task_id=f'clean_transform_data_{_table_name}',
        configuration={
            "query": {
                "query": "{% include '" + _sql_template_clean_transform_data + "' %}",
                "useLegacySql": False,
            }
        },
        params=params,
        location='US', 
        gcp_conn_id=_gcp_conn_id,
        retries=2,
        retry_delay=timedelta(minutes=5)
    )

    insert_cleaned_transformed_data_to_table_task = BigQueryInsertJobOperator(
        task_id=f'insert_cleaned_transformed_data_to_table_{_table_name}',
        configuration={
            "query": {
                "query": "{% include '" + _sql_template_insert_cleaned_transformed_data_to_table + "' %}",
                "useLegacySql": False,
            }
        },
        params=params,
        location='US', 
        gcp_conn_id=_gcp_conn_id,
        retries=2,
        retry_delay=timedelta(minutes=5)
    )

    insert_invalid_data_to_table_task = BigQueryInsertJobOperator(
        task_id=f'insert_invalid_data_to_table_{_table_name}',
        configuration={
            "query": {
                "query": "{% include '" + _sql_template_insert_invalid_data_to_table + "' %}",
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

    check_invalid_rows_and_notify_task = check_invalid_rows_and_notify.override(task_id=f'check_invalid_rows_and_notify_{_table_name}')(table_name=_table_name, dataset_name=_output_dataset, project_name=_project_name, gcp_conn_id=_gcp_conn_id, developer_email=_developer_email)

    max_timestamp = get_max_timestamp_task(gcp_conn_id=_gcp_conn_id, dataset_name=_output_dataset, table_name=_table_name)

    max_timestamp >> clean_transform_data_task >> [insert_cleaned_transformed_data_to_table_task, insert_invalid_data_to_table_task] 

    insert_cleaned_transformed_data_to_table_task >> drop_temp_table_task

    insert_invalid_data_to_table_task >> [check_invalid_rows_and_notify_task, drop_temp_table_task]

    # dqc = data_quality_check.override(task_id=f'data_quality_check_{_table_name}')(_table_name, _output_dataset)

    # insert_job = insert_job_task.override(task_id=f'insert_cleaned_{_table_name}_job_to_log')(gcp_conn_id=_gcp_conn_id, dataset_name=_output_dataset, table_name=_table_name)

    # max_timestamp >> insert_cleaned_data_to_table >> dqc >> insert_job