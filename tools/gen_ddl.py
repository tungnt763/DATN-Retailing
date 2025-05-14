from pathlib import Path
import json

LAYER = ['edw_cleaned', 'edw']
METADATA = ['clean_layer_table_info.json', 'edw_layer_table_info.json']

def generate_description(label):
    return {
        "loaded_batch": "Unix timestamp of the loaded batch",
        "loaded_part": "Partition date of the batch load",
        "batch_load_ts": "Timestamp when batch was loaded",
        "effective_start_date": "Effective start date (timestamp) of the record",
        "effective_end_date": "Effective end date (timestamp) of the record",
        "flag_active": "Flag to indicate if the record is active",
        "create_date": "Timestamp when record was created in cleaned layer",
        "create_task_id": "Airflow task ID that created this record",
        "create_task_run_id": "Airflow run ID that created this record",
        "update_date": "Timestamp when record was updated in cleaned layer",
        "update_task_id": "Airflow task ID that updated this record",
        "update_task_run_id": "Airflow run ID that updated this record"
    }.get(label, "")

for layer, metadata in zip(LAYER, METADATA):
    with open(f"dags/config/{metadata}", "r") as f:
        metadata = json.load(f)

    # Generate DDLs
    output_dir = Path(f"/Users/tungnt763/Documents/DATN-Retailing/dags/resources/sql_template/ddl/{layer}")
    output_dir.mkdir(parents=True, exist_ok=True)

    ddl_sql_list = []
    for table_key, table_info in metadata.items():
        table_name = table_info["physical_name"]
        lines = [f"CREATE TABLE IF NOT EXISTS `datn-retailing.{layer}.{table_name}` ("]
        
        for col in sorted(table_info["columns"], key=lambda x: int(x["index"])):
            name = col["physical_name"]
            typ = col["type"]
            mode = "NOT NULL" if col["mode"].upper() == "REQUIRED" else ""
            logical_name = col.get("logical_name_en", name)
            clmn_desc = f"{logical_name} of the {table_name} table"
            description_obj = {
                "clmn_lgcl_name_eng": logical_name,
                "clmn_desc": clmn_desc
            }
            description_str = json.dumps(description_obj).replace('"', '\\"')
            lines.append(f'    {name:<25} {typ} {mode} OPTIONS(description="{description_str}"),')

        # Standard + audit fields with full descriptions
        for meta_col, col_type in [
            ("loaded_batch", "STRING"),
            ("loaded_part", "DATE"),
            ("batch_load_ts", "TIMESTAMP"),
            ("effective_start_date", "TIMESTAMP"),
            ("effective_end_date", "TIMESTAMP"),
            ("flag_active", "STRING"),
            ("create_date", "TIMESTAMP"),
            ("create_task_id", "STRING"),
            ("create_task_run_id", "STRING"),
            ("update_date", "TIMESTAMP"),
            ("update_task_id", "STRING"),
            ("update_task_run_id", "STRING")
        ]:
            if (meta_col.split('_')[0] == "update") and layer == 'edw_cleaned':
                continue

            if meta_col in ["effective_start_date", "effective_end_date", "flag_active"]:
                if layer == 'edw_cleaned':
                    continue
                elif table_info["table_type"] == "fact":
                    continue
                elif table_info["scd_type"] == "scd1":
                    continue
            
            if 'load' in meta_col and layer == 'edw':
                continue

            description_obj = {
                "clmn_lgcl_name_eng": meta_col.replace("_", " ").title(),
                "clmn_desc": generate_description(meta_col)
            }
            description_str = json.dumps(description_obj).replace('"', '\\"')
            lines.append(f'    {meta_col:<25} {col_type} OPTIONS(description="{description_str}"),')


        lines[-1] = lines[-1].rstrip(',')
        lines.append(");")

        ddl_sql = "\n".join(lines)
        output_path = output_dir / f"{table_key}_ddl.sql"
        output_path.write_text(ddl_sql)
        ddl_sql_list.append(ddl_sql)

    output_path = output_dir / f"{layer}_ddl.sql"
    output_path.write_text("\n\n".join(ddl_sql_list))