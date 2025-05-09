from airflow.decorators import task, task_group
from lib.utils import get_schema_field_load_layer, list_all_file_name_gcs, get_schema_load_table, get_unix_timestamp_from_filename
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
    _sql_template_create_temp_table = os.path.join("resources", "sql_template", _dataset_name, "create_temp_table.sql")
    _sql_template_staging = os.path.join("resources", "sql_template", _dataset_name, "insert_staging_to_temp_table.sql")
    _sql_template_temp = os.path.join("resources", "sql_template", _dataset_name, "insert_temp_to_load_table.sql")
    _sql_template_drop_staging_temp_table = os.path.join("resources", "sql_template", _dataset_name, "drop_staging_temp_table.sql")

    # >>>>> Schema fields <<<<<
    _schema_fields_list = get_schema_field_load_layer(_table_name)
    _schema_columns, _columns = get_schema_load_table(_table_name)

    params={
        'project_name': _project_name,
        'dataset_name': _dataset_name,
        'table_name': _table_name,
        'schema_columns': _schema_columns,
        'columns': _columns,
    }

    source_objects_list = list_all_file_name_gcs(_table_name, _gcp_conn_id, _bucket_name, _prefix_name)

    create_load_dataset = BigQueryCreateEmptyDatasetOperator(
            task_id='create_load_dataset',
            gcp_conn_id=_gcp_conn_id,
            dataset_id=_dataset_name,
            project_id=_project_name,
            location='US',
            exists_ok=True,
        )

    create_temp_table = BigQueryInsertJobOperator(
        task_id=f'create_temp_table_{_table_name}',
        gcp_conn_id=_gcp_conn_id,
        configuration={
            "query": {
                "query": "{% include '" + _sql_template_create_temp_table + "' %}",
                "useLegacySql": False,
            }
        },
        params=params,
        location='US', 
    )
    

    if source_objects_list:
        load_data_to_staging = GCSToBigQueryOperator.partial(
            task_id=f'load_{_table_name}_gcs_to_staging',
            gcp_conn_id=_gcp_conn_id,
            bucket=_bucket_name,
            destination_project_dataset_table=f"{_project_name}.{_dataset_name}.{_table_name}_staging",
            schema_fields=_schema_fields_list,
            write_disposition='WRITE_TRUNCATE',
        ).expand(
            source_objects=[[file] for file in source_objects_list]
        )

        load_staging_to_temp = BigQueryInsertJobOperator.partial(
            task_id=f"load_{_table_name}_staging_to_temp",
            gcp_conn_id=_gcp_conn_id,
            configuration={
                "query": {
                    "query": "{% include '" + _sql_template_staging + "' %}",
                    "useLegacySql": False,
                }
            },
            location='US', 
        ).expand_kwargs([
            {
                "params": {
                    "project_name": _project_name,
                    "dataset_name": _dataset_name,
                    "table_name": _table_name,
                    "columns": _columns,
                    "loaded_batch": str(get_unix_timestamp_from_filename(file))
                }
            }
            for file in source_objects_list
        ])
    else:
        load_data_to_staging = EmptyOperator(task_id=f"skip_load_{_table_name}_gcs_to_staging")
        load_staging_to_temp = EmptyOperator(task_id=f"skip_load_{_table_name}_staging_to_temp")

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

    max_timestamp >> create_load_dataset >> create_temp_table >> load_data_to_staging >> load_staging_to_temp >> dqc >> load_temp_to_table >> drop_staging_temp_table >> insert_job

    # create_load_dataset >> load_data_to_bigquery >> dqc
    
    # dqc >> load_temp_to_table >> insert_job