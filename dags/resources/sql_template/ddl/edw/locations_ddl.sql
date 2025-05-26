CREATE TABLE IF NOT EXISTS `datn-retailing.edw.dim_locations` (
    lct_surr_key              STRING NOT NULL OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Location Surrogate Key\", \"clmn_desc\": \"Location Surrogate Key of the dim_locations table\"}"),
    lct_raw_city              STRING NOT NULL OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Raw City\", \"clmn_desc\": \"Raw City of the dim_locations table\"}"),
    lct_raw_cntry             STRING NOT NULL OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Raw Country\", \"clmn_desc\": \"Raw Country of the dim_locations table\"}"),
    lct_tr_city               STRING NOT NULL OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Translated City\", \"clmn_desc\": \"Translated City of the dim_locations table\"}"),
    lct_tr_cntry              STRING NOT NULL OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Translated Country\", \"clmn_desc\": \"Translated Country of the dim_locations table\"}"),
    create_date               TIMESTAMP OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Create Date\", \"clmn_desc\": \"Timestamp when record was created in cleaned layer\"}"),
    create_task_id            STRING OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Create Task Id\", \"clmn_desc\": \"Airflow task ID that created this record\"}"),
    create_task_run_id        STRING OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Create Task Run Id\", \"clmn_desc\": \"Airflow run ID that created this record\"}"),
    update_date               TIMESTAMP OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Update Date\", \"clmn_desc\": \"Timestamp when record was updated in cleaned layer\"}"),
    update_task_id            STRING OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Update Task Id\", \"clmn_desc\": \"Airflow task ID that updated this record\"}"),
    update_task_run_id        STRING OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Update Task Run Id\", \"clmn_desc\": \"Airflow run ID that updated this record\"}")
);