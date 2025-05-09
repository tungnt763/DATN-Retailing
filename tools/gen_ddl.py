from pathlib import Path
import json

def generate_description(label):
    return {
        "loaded_batch": "Unix timestamp of the loaded batch",
        "loaded_part": "Partition date of the batch load",
        "batch_load_ts": "Timestamp when batch was loaded",
        "create_date": "Timestamp when record was created in cleaned layer",
        "create_task_id": "Airflow task ID that created this record",
        "create_task_run_id": "Airflow run ID that created this record"
    }.get(label, "")

with open("dags/config/clean_layer_table_info.json", "r") as f:
    clean_meta = json.load(f)

# Generate DDLs
output_dir = Path("/Users/tungnt763/Documents/DATN-Retailing/dags/resources/sql_template/ddl/edw_cleaned")
output_dir.mkdir(parents=True, exist_ok=True)

ddl_sql_list = []
for table_name, table_info in clean_meta.items():
    lines = [f"CREATE TABLE IF NOT EXISTS `datn-retailing.edw_cleaned.{table_name}` ("]
    
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
        ("create_date", "TIMESTAMP"),
        ("create_task_id", "STRING"),
        ("create_task_run_id", "STRING")
    ]:
        description_obj = {
            "clmn_lgcl_name_eng": meta_col.replace("_", " ").title(),
            "clmn_desc": generate_description(meta_col)
        }
        description_str = json.dumps(description_obj).replace('"', '\\"')
        lines.append(f'    {meta_col:<25} {col_type} OPTIONS(description="{description_str}"),')


    lines[-1] = lines[-1].rstrip(',')
    lines.append(");")

    ddl_sql = "\n".join(lines)
    output_path = output_dir / f"{table_name}_ddl.sql"
    output_path.write_text(ddl_sql)
    ddl_sql_list.append(ddl_sql)

output_path = output_dir / "edw_cleaned_ddl.sql"
output_path.write_text("\n\n".join(ddl_sql_list))