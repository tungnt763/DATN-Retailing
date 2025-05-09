CREATE TABLE IF NOT EXISTS `datn-retailing.edw_cleaned.customers` (
    cstmr_id                  STRING NOT NULL OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Customer ID\", \"clmn_desc\": \"Customer ID of the customers table\"}"),
    cstmr_name                STRING  OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Name\", \"clmn_desc\": \"Name of the customers table\"}"),
    cstmr_eml                 STRING  OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Email\", \"clmn_desc\": \"Email of the customers table\"}"),
    cstmr_tel                 STRING NOT NULL OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Telephone\", \"clmn_desc\": \"Telephone of the customers table\"}"),
    cstmr_city                STRING  OPTIONS(description="{\"clmn_lgcl_name_eng\": \"City\", \"clmn_desc\": \"City of the customers table\"}"),
    cstmr_cntry               STRING  OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Country\", \"clmn_desc\": \"Country of the customers table\"}"),
    cstmr_gender              STRING  OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Gender\", \"clmn_desc\": \"Gender of the customers table\"}"),
    cstmr_brthdy              DATE  OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Date of Birth\", \"clmn_desc\": \"Date of Birth of the customers table\"}"),
    cstmr_job_title           STRING  OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Job Title\", \"clmn_desc\": \"Job Title of the customers table\"}"),
    loaded_batch              STRING OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Loaded Batch\", \"clmn_desc\": \"Unix timestamp of the loaded batch\"}"),
    loaded_part               DATE OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Loaded Part\", \"clmn_desc\": \"Partition date of the batch load\"}"),
    batch_load_ts             TIMESTAMP OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Batch Load Ts\", \"clmn_desc\": \"Timestamp when batch was loaded\"}"),
    create_date               TIMESTAMP OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Create Date\", \"clmn_desc\": \"Timestamp when record was created in cleaned layer\"}"),
    create_task_id            STRING OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Create Task Id\", \"clmn_desc\": \"Airflow task ID that created this record\"}"),
    create_task_run_id        STRING OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Create Task Run Id\", \"clmn_desc\": \"Airflow run ID that created this record\"}")
);