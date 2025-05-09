# >>>>>    Extract   <<<<<
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

# >>>>>    Common    <<<<<
def replace_sql_values(sql_template_path: str, replacements: dict={}) -> str:

    with open(sql_template_path, 'r') as file:
        sql_template = file.read()
    try:
        sql_query = sql_template if not replacements else sql_template.format(**replacements)
        return sql_query
    except KeyError as e:
        print(f'Missing key in replacements: ', e)
        return None

def read_metadata(metadata):
    import os
    import json

    metadata_file_path = os.path.join(os.getenv('AIRFLOW_HOME'), 'dags', 'config', f'{metadata}.json')
    with open(metadata_file_path, 'r') as f:
        metadata = json.load(f)

    return metadata

# >>>>> Loaded layer <<<<<
def list_all_file_name_gcs(table_name, gcp_conn_id, bucket_name, prefix_name):
    from airflow.providers.google.cloud.hooks.gcs import GCSHook

    hook = GCSHook(gcp_conn_id=gcp_conn_id)

    blobs = hook.list(bucket_name, prefix=prefix_name)

    if blobs:
        return blobs 
    return None

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

def get_schema_load_table(table_name):
    import os
    import json

    env_file_path = os.path.join(os.getenv('AIRFLOW_HOME'), 'dags', 'config', 'load_layer_table_info.json')
    with open(env_file_path, 'r') as f:
        loading_layer_table_info = json.load(f)

    table_data = loading_layer_table_info.get(table_name)

    ddl_lines = []
    columns = []
    for col in sorted(table_data["columns"], key=lambda x: int(x["index"])):
        col_name = col["physical_name"]
        col_type = col["type"]
        mode = "NOT NULL" if col["mode"].upper() == "REQUIRED" else ""

        description_obj = {
            "clmn_lgcl_name_eng": col["logical_name_en"],
            "clmn_desc": ""
        }
        
        description_str = json.dumps(description_obj).replace('"', '\\"')
        line = f'{col_name:<25} {col_type} {mode} OPTIONS(description="{description_str}")'
        ddl_lines.append(line)
        columns.append(col_name)

    return [',\n    '.join(ddl_lines), ',\n    '.join(columns)]

def get_unix_timestamp_from_filename(filename: str) -> int:
    from datetime import datetime

    dt = datetime.strptime(filename.split('.')[0].split('_')[-1], "%Y%m%dT%H%M%S")
    return int(dt.timestamp())

# >>>>> Cleaned layer <<<<<

def get_clean_expressions_for_table(table_name, metadata_file_name):
    metadata = read_metadata(metadata_file_name)

    columns = metadata[table_name]["columns"]
    cleaned_exprs = []
    selected_cols = []
    pk_cols = []
    cols = []

    for col in columns:
        name = col["physical_name"]
        typ = col["type"]
        default = col.get("default_value", "")
        nullable = col["mode"] != "REQUIRED"
        is_pk = col.get("pk") == "Y"

        base = f"TRIM({name})"
        
        regax = col.get("regax", "")
        if regax:
            check = f"REGEXP_CONTAINS({base}, r'{regax}')"
        else:
            check = "TRUE"

        if typ in ["NUMERIC", "FLOAT64", "INT64"]:
            cast = f"SAFE_CAST({base} AS {typ})"
        elif typ == "DATE":
            cast = f"SAFE.PARSE_DATE('%Y-%m-%d', {base})"
        elif typ == "DATETIME":
            cast = f"SAFE.PARSE_DATETIME('%Y-%m-%d %H-%M-%S', {base})"
        else:
            cast = f"INITCAP({base})"

        if not nullable and default:
            if typ in ["NUMERIC", "FLOAT64", "INT64"]:
                expr = (
                    f"CASE WHEN {base} IS NULL OR {base} = '' OR NOT ({check}) "
                    f"THEN {default} ELSE {cast} END AS {name}"
                )
            else:
                expr = (
                    f"CASE WHEN {base} IS NULL OR {base} = '' OR NOT ({check}) "
                    f"THEN '{default}' ELSE {cast} END AS {name}"
                )
        elif nullable and default not in ["", "NULL"]:
            if typ in ["NUMERIC", "FLOAT64", "INT64"]:
                expr = (
                    f"CASE WHEN {base} IS NULL OR {base} = '' OR NOT ({check}) "
                    f"THEN {default} ELSE {cast} END AS {name}"
                )
            else:
                expr = (
                    f"CASE WHEN {base} IS NULL OR {base} = '' OR NOT ({check}) "
                    f"THEN '{default}' ELSE {cast} END AS {name}"
                )
        else:
            expr = f"{cast} AS {name}"

        cols.append(name)
        cleaned_exprs.append(expr)
        selected_cols.append(name)
        if is_pk:
            pk_cols.append(name)

    selected_cols += ["loaded_batch", "loaded_part", "batch_load_ts"]
    if not pk_cols:
        pk_cols = cols

    return [cleaned_exprs, selected_cols, pk_cols, cols]