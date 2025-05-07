from airflow.decorators import task, task_group
from lib.utils import get_schema_field_load_layer, list_all_file_name_gcs
from resources.python_task.loading_layer import check_load
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

@task_group(group_id='loading_layer')
def loading_layer(**kwargs):
    _table_name = kwargs.get('table_name')
    _project_name = kwargs.get('project')
    _dataset_name = kwargs.get('load_dataset')

    _bucket_name = kwargs.get('bucket_name')
    _prefix_name = kwargs.get('prefix_name')
    _gcp_conn_id = kwargs.get('gcp_conn_id')

    schema_fields_list = get_schema_field_load_layer(_table_name)

    source_objects_list = list_all_file_name_gcs(_table_name, _gcp_conn_id, _bucket_name, _prefix_name)

    load_data_to_bigquery = GCSToBigQueryOperator(
        task_id=f'load_{_table_name}_gcs_to_bigquery',
        gcp_conn_id=_gcp_conn_id,
        bucket=_bucket_name,
        source_objects=source_objects_list,
        destination_project_dataset_table=f"{_project_name}.{_dataset_name}.{_table_name}_temp",
        schema_fields= schema_fields_list,
        write_disposition='WRITE_TRUNCATE',
    )

    check_load_task = check_load.override(task_id=f'check_{_table_name}_load')(_table_name, _dataset_name)

    load_data_to_bigquery >> check_load_task