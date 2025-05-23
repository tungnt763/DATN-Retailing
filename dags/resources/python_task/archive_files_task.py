from airflow.decorators import task
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.utils.dates import days_ago
from lib.utils import get_file_and_loaded_batch
@task(provide_context=True)
def archive_gcs_files(bucket_name, table_name, gcp_conn_id, prefix_name, **context):

    file, loaded_batch = get_file_and_loaded_batch(table_name, gcp_conn_id, bucket_name, prefix_name)

    if not file:
        print(f"No file found to archive for table {table_name} with prefix {prefix_name}")
        return

    execution_date = context['execution_date']
    archive_date = execution_date.strftime('%Y-%m-%d')
    archived_prefix = f'archived/{table_name}'

    gcs_hook = GCSHook(gcp_conn_id=gcp_conn_id)
        
    archive_path = f"{archived_prefix}/{archive_date}/{file.split('/')[-1]}"
        
    if gcs_hook.exists(bucket_name, file):
        # Copy file to archive path
        gcs_hook.copy(
            source_bucket=bucket_name,
            source_object=file,
            destination_bucket=bucket_name,
            destination_object=archive_path
        )
        # Delete original file
        gcs_hook.delete(bucket_name=bucket_name, object_name=file)
        print(f"Archived {file} to {archive_path}")
    else:
        print(f"File {file} does not exist in bucket {bucket_name}")
