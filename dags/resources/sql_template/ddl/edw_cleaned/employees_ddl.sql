CREATE TABLE IF NOT EXISTS `datn-retailing.edw_cleaned.employees` (
    emply_id                  STRING NOT NULL OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Employee ID\", \"clmn_desc\": \"Employee ID of the employees table\"}"),
    emply_str_id              STRING  OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Store ID\", \"clmn_desc\": \"Store ID of the employees table\"}"),
    emply_name                STRING  OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Name\", \"clmn_desc\": \"Name of the employees table\"}"),
    emply_pstn                STRING  OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Position\", \"clmn_desc\": \"Position of the employees table\"}"),
    loaded_batch              STRING OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Loaded Batch\", \"clmn_desc\": \"Unix timestamp of the loaded batch\"}"),
    loaded_part               DATE OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Loaded Part\", \"clmn_desc\": \"Partition date of the batch load\"}"),
    batch_load_ts             TIMESTAMP OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Batch Load Ts\", \"clmn_desc\": \"Timestamp when batch was loaded\"}"),
    create_date               TIMESTAMP OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Create Date\", \"clmn_desc\": \"Timestamp when record was created in cleaned layer\"}"),
    create_task_id            STRING OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Create Task Id\", \"clmn_desc\": \"Airflow task ID that created this record\"}"),
    create_task_run_id        STRING OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Create Task Run Id\", \"clmn_desc\": \"Airflow run ID that created this record\"}")
);