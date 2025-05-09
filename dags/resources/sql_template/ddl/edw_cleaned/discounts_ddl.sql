CREATE TABLE IF NOT EXISTS `datn-retailing.edw_cleaned.discounts` (
    dscnt_start_date          DATE NOT NULL OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Start Date\", \"clmn_desc\": \"Start Date of the discounts table\"}"),
    dscnt_end_date            DATE  OPTIONS(description="{\"clmn_lgcl_name_eng\": \"End Date\", \"clmn_desc\": \"End Date of the discounts table\"}"),
    dscnt_value               NUMERIC NOT NULL OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Discount\", \"clmn_desc\": \"Discount of the discounts table\"}"),
    dscnt_desc                STRING  OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Description\", \"clmn_desc\": \"Description of the discounts table\"}"),
    dscnt_ctgry               STRING  OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Category\", \"clmn_desc\": \"Category of the discounts table\"}"),
    dscnt_sub_ctgry           STRING  OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Sub Category\", \"clmn_desc\": \"Sub Category of the discounts table\"}"),
    loaded_batch              STRING OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Loaded Batch\", \"clmn_desc\": \"Unix timestamp of the loaded batch\"}"),
    loaded_part               DATE OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Loaded Part\", \"clmn_desc\": \"Partition date of the batch load\"}"),
    batch_load_ts             TIMESTAMP OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Batch Load Ts\", \"clmn_desc\": \"Timestamp when batch was loaded\"}"),
    create_date               TIMESTAMP OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Create Date\", \"clmn_desc\": \"Timestamp when record was created in cleaned layer\"}"),
    create_task_id            STRING OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Create Task Id\", \"clmn_desc\": \"Airflow task ID that created this record\"}"),
    create_task_run_id        STRING OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Create Task Run Id\", \"clmn_desc\": \"Airflow run ID that created this record\"}")
);