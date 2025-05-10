from airflow.decorators import task
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.utils.dates import days_ago

@task(provide_context=True)
def archive_gcs_files(bucket_name, table_name, gcp_conn_id, **context):

    execution_date = context['execution_date']
    archive_date = execution_date.strftime('%Y-%m-%d')
    raw_prefix = f'raw/{table_name}'
    archived_prefix = f'archived/{table_name}'

    gcs_hook = GCSHook(gcp_conn_id=gcp_conn_id)

    source_files = gcs_hook.list(bucket_name, prefix=raw_prefix)

    for file_path in source_files:
        archive_path = f"{archived_prefix}/{archive_date}/{file_path.split('/')[-1]}"
        
        gcs_hook.copy(
            source_bucket=bucket_name,
            source_object=file_path,
            destination_bucket=bucket_name,
            destination_object=archive_path
        )

        gcs_hook.delete(bucket_name=bucket_name, object_name=file_path)

    print(f"Archived {len(source_files)} files from {raw_prefix} to {archived_prefix}/{archive_date}/")
