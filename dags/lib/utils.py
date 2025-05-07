def archive_file(DATA_FOLDER, file_path_pattern, **kwargs):
    import os
    import shutil
    import glob

    archive_dir = os.path.join(DATA_FOLDER, 'archive')
    if not os.path.exists(archive_dir):
        os.makedirs(archive_dir)

    matches = glob.glob(file_path_pattern)
    for file_path in matches:
        file_name = os.path.basename(file_path)
        shutil.copy(file_path, os.path.join(archive_dir, file_name))
        os.remove(file_path)
        print(f"--- Moved {file_name} to archive folder ---")

def load_db_env():
    import os
    import json

    # Load environment variables from JSON file
    env_file_path = os.path.join(os.getenv('AIRFLOW_HOME'), 'dags', 'config', 'db_env.json')
    with open(env_file_path, 'r') as f:
        env_vars = json.load(f)

    return env_vars

def get_table_names():
    import os
    import json

    # Load table names from JSON file
    env_file_path = os.path.join(os.getenv('AIRFLOW_HOME'), 'dags', 'config', 'load_layer_table_info.json')
    with open(env_file_path, 'r') as f:
        table_names = json.load(f)

    table_name_list = []
    for key, value in table_names.items():
        table_name_list.append(value.get('physical_name'))

    return table_name_list

def get_schema_field_load_layer(table_name):
    import os
    import json

    # Load schema field names from JSON file
    env_file_path = os.path.join(os.getenv('AIRFLOW_HOME'), 'dags', 'config', 'load_layer_table_info.json')
    with open(env_file_path, 'r') as f:
        loading_layer_table_info = json.load(f)

    schema_field = []
    for column_info in loading_layer_table_info.get(table_name).get('columns'):
        schema_field.append({
            'name': column_info.get('physical_name'),
            'type': column_info.get('type'),
            'mode': 'NULLABLE'
        })

    return schema_field

def list_all_file_name_gcs(table_name, gcp_conn_id, bucket_name, prefix_name):
    from airflow.providers.google.cloud.hooks.gcs import GCSHook

    hook = GCSHook(gcp_conn_id=gcp_conn_id)

    blobs = hook.list(bucket_name, prefix=prefix_name)

    if blobs:
        return blobs 
    return None
