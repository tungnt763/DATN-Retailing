CREATE TABLE IF NOT EXISTS `datn-retailing.edw_cleaned.stores` (
    str_id                    STRING NOT NULL OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Store ID\", \"clmn_desc\": \"Store ID of the stores table\"}"),
    str_cntry                 STRING  OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Country\", \"clmn_desc\": \"Country of the stores table\"}"),
    str_city                  STRING  OPTIONS(description="{\"clmn_lgcl_name_eng\": \"City\", \"clmn_desc\": \"City of the stores table\"}"),
    str_name                  STRING  OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Store Name\", \"clmn_desc\": \"Store Name of the stores table\"}"),
    str_emply_num             INT64  OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Number of Employees\", \"clmn_desc\": \"Number of Employees of the stores table\"}"),
    str_zip_cd                STRING  OPTIONS(description="{\"clmn_lgcl_name_eng\": \"ZIP Code\", \"clmn_desc\": \"ZIP Code of the stores table\"}"),
    str_lat                   NUMERIC NOT NULL OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Latitude\", \"clmn_desc\": \"Latitude of the stores table\"}"),
    str_lon                   NUMERIC NOT NULL OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Longitude\", \"clmn_desc\": \"Longitude of the stores table\"}"),
    loaded_batch              STRING OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Loaded Batch\", \"clmn_desc\": \"Unix timestamp of the loaded batch\"}"),
    loaded_part               DATE OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Loaded Part\", \"clmn_desc\": \"Partition date of the batch load\"}"),
    batch_load_ts             TIMESTAMP OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Batch Load Ts\", \"clmn_desc\": \"Timestamp when batch was loaded\"}"),
    create_date               TIMESTAMP OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Create Date\", \"clmn_desc\": \"Timestamp when record was created in cleaned layer\"}"),
    create_task_id            STRING OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Create Task Id\", \"clmn_desc\": \"Airflow task ID that created this record\"}"),
    create_task_run_id        STRING OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Create Task Run Id\", \"clmn_desc\": \"Airflow run ID that created this record\"}")
);