from airflow.decorators import task
from lib.job_control import get_max_timestamp

@task(provide_context=True, task_id=f'get_max_timestamp')
def get_max_timestamp_task(gcp_conn_id, dataset_name, table_name, **context):
    max_timestamp = get_max_timestamp(gcp_conn_id, dataset_name, table_name)

    if max_timestamp:
        context['ti'].xcom_push(key='max_timestamp', value=max_timestamp)
        print(f">> {table_name}'s max timestamp: {max_timestamp}")
    else:
        raise Exception("GET max timestamp failed, marking task as failed.")