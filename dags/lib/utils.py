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

def get_file_and_loaded_batch(table_name, gcp_conn_id, bucket_name, prefix_name):
    blobs = list_all_file_name_gcs(table_name, gcp_conn_id, bucket_name, prefix_name)

    file = ''
    loaded_batch = 0

    if blobs:
        for file_check in blobs:
            if get_unix_timestamp_from_filename(file_check) > loaded_batch:
                file = file_check
                loaded_batch = get_unix_timestamp_from_filename(file_check)
    
    return file, loaded_batch

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

# >>>>>   Dim table   <<<<<
def get_clean_expressions_for_table(table_name, metadata_file_name, input_dataset, output_dataset, project_name):
    metadata = read_metadata(metadata_file_name)

    columns = metadata[table_name]["columns"]
    cleaned_column_expressions = []
    selected_columns = []
    pk_expr = []

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
            if col.get("format") == "upper":
                cast = f"UPPER({base})"
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

        cleaned_column_expressions.append(expr)
        selected_columns.append(name)
        if is_pk:
            pk_expr.append(name)

    if not pk_expr:
        pk_expr = selected_columns
    selected_columns += ["loaded_batch", "loaded_part", "batch_load_ts"]

    return {
        "project_name": project_name,
        "input_dataset": input_dataset,
        "output_dataset": output_dataset,
        "table_name": table_name,
        "cleaned_column_expressions": ',\n        '.join(cleaned_column_expressions),
        "columns": ',\n        '.join(selected_columns),
        "selected_columns": ',\n    '.join(selected_columns),
        "pk_expr": ', '.join(pk_expr),
    }

# >>>>>   Fact table  <<<<<
def get_clean_expressions_for_fact_table(table_name, metadata_file_name, input_dataset, output_dataset, project_name):
    metadata = read_metadata(metadata_file_name)

    columns = metadata[table_name]["columns"]
    cast_exprs = []
    col_names = []
    pk_expr = []
    for col in columns:
        col_name = col["physical_name"]
        col_type = col["type"]
        col_format = col.get("format", "").lower()
        expr = f"TRIM({col_name})"

        if col.get("pk") == "Y":
            pk_expr.append(col_name)

        # Apply format transformations
        if col_format == "upper":
            expr = f"UPPER({expr})"
        elif col_format == "initcap":
            expr = f"INITCAP({expr})"

        # Apply SAFE_CAST
        expr = f"SAFE_CAST({expr} AS {col_type})"
        cast_exprs.append(f"{expr} AS {col_name}")
        col_names.append(col_name)

    return {
        "project_name": project_name,
        "input_dataset": input_dataset,
        "output_dataset": output_dataset,
        "table_name": table_name,
        "cast_exprs": ',\n    '.join(cast_exprs),
        "col_names": ',\n    '.join(col_names),
        "pk_expr": ', '.join(pk_expr),
    }
    
# >>>>>   EDW layer   <<<<<

# >>>>>   Dim table   <<<<<
def get_edw_expressions_for_table(table_name, metadata_file_name, input_dataset, output_dataset, project_name):
    import json
    from pathlib import Path

    # Load JSON data
    metadata = read_metadata(metadata_file_name)

    table_info = metadata[table_name]
    columns = table_info["columns"]

    old_columns = ",\n    ".join([col["physical_name"] if col["method"] == "" else f"{col['method']} AS {col['physical_name']}" for col in columns if col["pk"] != "Y"])
    old_columns_in_row = ", ".join([col["physical_name"] for col in columns if col["pk"] != "Y"])
    old_columns_except_method = ",\n    ".join([col["physical_name"] for col in columns if col["pk"] != "Y"])

    natural_keys = [col["physical_name"] for col in columns if col["nk"] == "Y"]
    natural_key_expr = "\n    AND ".join([f"target.{nk} = source.{nk}" for nk in natural_keys])

    columns_except_natural_key_expr = ",\n        ".join([f"{col['physical_name']} = source.{col['physical_name']}" for col in columns if col["pk"] != "Y" and col["nk"] != "Y"])

    columns_except_natural_key_equal_expr = "\n    AND ".join([f"target.{col['physical_name']} = source.{col['physical_name']}" for col in columns if col["pk"] != "Y" and col["nk"] != "Y"])

    new_columns = ", ".join([col["physical_name"] for col in columns])

    params = {
        "project_name": project_name,
        "input_dataset": input_dataset,
        "input_table": table_name,
        "output_dataset": output_dataset,
        "output_table": table_info["physical_name"],
        "old_columns": old_columns,
        "old_columns_in_row": old_columns_in_row,
        "old_columns_except_method": old_columns_except_method,
        "natural_key_expr": natural_key_expr,
        "columns_except_natural_key_equal_expr": columns_except_natural_key_equal_expr,
        "columns_except_natural_key_expr": columns_except_natural_key_expr,
        "new_columns": new_columns,
        "scd_type": table_info["scd_type"],
    }

    return params

# >>>>>   Fact table  <<<<<
def get_edw_expressions_for_fact_table(table_name, metadata_file_name, input_dataset, output_dataset, project_name):
    metadata = read_metadata(metadata_file_name)

    table_info = metadata[table_name]
    columns = table_info["columns"]
    col_names = [col["physical_name"] for col in columns]
    
    # method expressions (SELECT col AS alias)
    method_exprs = [
        f"{col.get('method')} AS {col['physical_name']}" if col.get('method') else col['physical_name']
        for col in columns
    ]
    
    # schema columns with type (CREATE TABLE ...)
    schema_columns = [
        f"{col['physical_name']} {col['type']}" for col in columns
    ]

    # array_agg expressions (for merge/select latest)
    array_agg_exprs = [
        f"ARRAY_AGG({col['physical_name']} ORDER BY trn_dt_key DESC, trn_hr DESC, flg_if DESC)[ORDINAL(1)] AS {col['physical_name']}"
        for col in columns if col.get('pk') != "Y"
    ]

    # primary keys
    pk_expr = [col["physical_name"] for col in columns if col.get("pk") == "Y"]

    return {
        "project_name": project_name,
        "input_dataset": input_dataset,
        "input_table": table_name,
        "output_dataset": output_dataset,
        "output_table": table_info["physical_name"],
        "col_names": ',\n    '.join(col_names),
        "method_exprs": ',\n    '.join(method_exprs),
        "schema_columns": ',\n    '.join(schema_columns),
        "array_agg_exprs": ',\n    '.join(array_agg_exprs),
        "pk_expr": ',\n    '.join(pk_expr),
    }
