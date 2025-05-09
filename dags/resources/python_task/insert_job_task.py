from lib.job_control import insert_log
from airflow.decorators import task

@task(provide_context=True)
def insert_job_task(gcp_conn_id, dataset_name, table_name, **context):
    log = insert_log(
        gcp_conn_id=gcp_conn_id,
        task_id=context['ti'].task_id,
        dataset_name=dataset_name,
        table_name=table_name,
        rundate=context['ds']
    )

    if log:
        print(f'>> Job control updated successfully')
    else:
        raise Exception("Log insertion failed, marking task as failed.")