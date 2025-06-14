from airflow.decorators import task, task_group
from lib.utils import get_schema_field_load_layer, list_all_file_name_gcs, get_schema_load_table, get_file_and_loaded_batch
from resources.python_task.data_quality_check import data_quality_check
from resources.python_task.get_max_timestamp_task import get_max_timestamp_task
from resources.python_task.insert_job_task import insert_job_task
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.empty import EmptyOperator

import os
from datetime import datetime, timezone

@task_group(group_id='loading_layer')
def loading_layer(**kwargs):
    # >>>>> Variables <<<<<
    _table_name = kwargs.get('table_name')
    _project_name = kwargs.get('project')
    _dataset_name = kwargs.get('load_dataset')

    _bucket_name = kwargs.get('bucket_name')
    _prefix_name = kwargs.get('prefix_name')
    _gcp_conn_id = kwargs.get('gcp_conn_id')

    # >>>>> SQL templates <<<<<
    _sql_template_temp = os.path.join("resources", "sql_template", _dataset_name, "insert_temp_to_load_table.sql")
    _sql_template_drop_staging_temp_table = os.path.join("resources", "sql_template", _dataset_name, "drop_staging_temp_table.sql")

    # >>>>> Schema fields <<<<<
    _schema_fields_list = get_schema_field_load_layer(_table_name)
    _schema_columns, _columns = get_schema_load_table(_table_name)

    params = {
        'project_name': _project_name,
        'dataset_name': _dataset_name,
        'table_name': _table_name,
        'schema_columns': _schema_columns,
        'columns': _columns,
    }

    @task(provide_context=True)
    def get_file_path(**context):
        return context["dag_run"].conf.get("file_path")

    file_path = get_file_path()

    create_load_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_load_dataset',
        gcp_conn_id=_gcp_conn_id,
        dataset_id=_dataset_name,
        project_id=_project_name,
        location='US',
        exists_ok=True,
    )

    load_data_to_temp = GCSToBigQueryOperator(
        task_id=f"load_{_table_name}_gcs_to_temp",
        gcp_conn_id=_gcp_conn_id,
        bucket=_bucket_name,
        source_objects=[file_path],
        destination_project_dataset_table=f"{_project_name}.{_dataset_name}.{_table_name}_temp_{{{{ dag_run.conf['loaded_batch'] }}}}",
        schema_fields=_schema_fields_list,
        write_disposition="WRITE_TRUNCATE",
    )

    load_temp_to_table = BigQueryInsertJobOperator(
        task_id=f'load_temp_to_{_table_name}',
        configuration={
            "query": {
                "query": "{% include '" + _sql_template_temp + "' %}",
                "useLegacySql": False,
            }
        },
        params=params,
        location='US', 
        gcp_conn_id=_gcp_conn_id
    )

    drop_staging_temp_table = BigQueryInsertJobOperator(
        task_id=f'drop_staging_temp_table_{_table_name}',
        gcp_conn_id=_gcp_conn_id,
        configuration={
            "query": {
                "query": "{% include '" + _sql_template_drop_staging_temp_table + "' %}",
                "useLegacySql": False,
            }
        },
        params=params,
        location='US', 
    )

    max_timestamp = get_max_timestamp_task(gcp_conn_id=_gcp_conn_id, dataset_name=_dataset_name, table_name=_table_name)

    dqc = data_quality_check.override(task_id=f'data_quality_check_{_table_name}')(_table_name, _dataset_name)

    insert_job = insert_job_task.override(task_id=f'insert_loaded_{_table_name}_job_to_log')(gcp_conn_id=_gcp_conn_id, dataset_name=_dataset_name, table_name=_table_name)

    file_path >> max_timestamp >> create_load_dataset >> load_data_to_temp >> load_temp_to_table >> dqc >>  drop_staging_temp_table >> insert_job