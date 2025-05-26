CREATE TABLE IF NOT EXISTS `datn-retailing.edw_cleaned.locations` (
    lct_raw_city              STRING NOT NULL OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Raw City\", \"clmn_desc\": \"Raw City of the locations table\"}"),
    lct_raw_cntry             STRING NOT NULL OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Raw Country\", \"clmn_desc\": \"Raw Country of the locations table\"}"),
    lct_tr_city               STRING NOT NULL OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Translated City\", \"clmn_desc\": \"Translated City of the locations table\"}"),
    lct_tr_cntry              STRING NOT NULL OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Translated Country\", \"clmn_desc\": \"Translated Country of the locations table\"}"),
    loaded_batch              STRING OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Loaded Batch\", \"clmn_desc\": \"Unix timestamp of the loaded batch\"}"),
    loaded_part               DATE OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Loaded Part\", \"clmn_desc\": \"Partition date of the batch load\"}"),
    batch_load_ts             TIMESTAMP OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Batch Load Ts\", \"clmn_desc\": \"Timestamp when batch was loaded\"}"),
    create_date               TIMESTAMP OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Create Date\", \"clmn_desc\": \"Timestamp when record was created in cleaned layer\"}"),
    create_task_id            STRING OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Create Task Id\", \"clmn_desc\": \"Airflow task ID that created this record\"}"),
    create_task_run_id        STRING OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Create Task Run Id\", \"clmn_desc\": \"Airflow run ID that created this record\"}")
);