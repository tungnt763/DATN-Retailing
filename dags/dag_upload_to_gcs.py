import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup
from lib.utils import archive_file, get_table_names

DATA_FOLDER = os.path.join(os.getenv('AIRFLOW_HOME', '/opt/airflow/'), 'include', 'dataset')
TABLE_NAMES = get_table_names()

default_args = {
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

def decide_branch(file_path_pattern, upload_task_id, alert_task_id, **kwargs):
    import glob
    matches = glob.glob(file_path_pattern)
    if matches:
        return upload_task_id
    else:
        return alert_task_id

@dag(
    schedule_interval='30 0 * * *',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=['gcs', 'upload', 'branch'],
)
def dag_upload_to_gcs_grouped():

    with TaskGroup(group_id='upload_to_gcs_group') as upload_to_gcs_group:

        for table_name in TABLE_NAMES:

            with TaskGroup(group_id=f'{table_name}_upload_group') as tg:

                relative_pattern = f'{table_name}*.csv'
                absolute_pattern = os.path.join(DATA_FOLDER, relative_pattern)

                wait = FileSensor(
                    task_id=f'wait_for_{table_name}_csv',
                    fs_conn_id='fs_default',
                    filepath=relative_pattern,
                    poke_interval=60,
                    timeout=300,
                    mode='reschedule',
                    soft_fail=True,
                )

                upload = LocalFilesystemToGCSOperator(
                    task_id=f'upload_{table_name}_csv_to_gcs',
                    src=os.path.join(DATA_FOLDER, f'{table_name}*.csv'),
                    dst=f'raw/{table_name}/{table_name}_{{{{ ts_nodash }}}}.csv',
                    bucket='retailing_data',
                    gcp_conn_id='gcp',
                    mime_type='text/csv',
                )

                alert = EmptyOperator(task_id=f'alert_no_{table_name}_file')

                # ✅ Khai báo sau khi đã có upload & alert
                branch = BranchPythonOperator(
                    task_id=f'branch_check_{table_name}_file',
                    python_callable=decide_branch,
                    op_kwargs={
                        'file_path_pattern': absolute_pattern,
                        'upload_task_id': upload.task_id,
                        'alert_task_id': alert.task_id,
                    },
                )

                archive = BranchPythonOperator(  # hoặc đổi sang PythonOperator nếu không cần nhánh
                    task_id=f'archive_{table_name}_csv',
                    python_callable=archive_file,
                    op_kwargs={
                        'DATA_FOLDER': DATA_FOLDER,
                        'file_path_pattern': absolute_pattern,
                    },
                )

                done = EmptyOperator(task_id=f'done_{table_name}')

                wait >> branch
                branch >> upload >> archive >> done
                branch >> alert >> done

    @task(trigger_rule="all_done")
    def all_done():
        print("All tasks in the group have completed.")
        print(os.getenv('AIRFLOW_HOME'))

    end = all_done()
    upload_to_gcs_group >> end

dag_upload_to_gcs_grouped()
