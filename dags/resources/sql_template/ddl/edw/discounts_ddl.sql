CREATE TABLE IF NOT EXISTS `datn-retailing.edw.dim_discounts` (
    dscnt_surr_key            STRING NOT NULL OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Discount Surrogate Key\", \"clmn_desc\": \"Discount Surrogate Key of the dim_discounts table\"}"),
    dscnt_start_date          DATE NOT NULL OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Start Date\", \"clmn_desc\": \"Start Date of the dim_discounts table\"}"),
    dscnt_end_date            DATE  OPTIONS(description="{\"clmn_lgcl_name_eng\": \"End Date\", \"clmn_desc\": \"End Date of the dim_discounts table\"}"),
    dscnt_value               NUMERIC NOT NULL OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Discount\", \"clmn_desc\": \"Discount of the dim_discounts table\"}"),
    dscnt_desc                STRING  OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Description\", \"clmn_desc\": \"Description of the dim_discounts table\"}"),
    dscnt_ctgry               STRING  OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Category\", \"clmn_desc\": \"Category of the dim_discounts table\"}"),
    dscnt_sub_ctgry           STRING  OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Sub Category\", \"clmn_desc\": \"Sub Category of the dim_discounts table\"}"),
    effective_start_date      TIMESTAMP OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Effective Start Date\", \"clmn_desc\": \"Effective start date (timestamp) of the record\"}"),
    effective_end_date        TIMESTAMP OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Effective End Date\", \"clmn_desc\": \"Effective end date (timestamp) of the record\"}"),
    flag_active               STRING OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Flag Active\", \"clmn_desc\": \"Flag to indicate if the record is active\"}"),
    create_date               TIMESTAMP OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Create Date\", \"clmn_desc\": \"Timestamp when record was created in cleaned layer\"}"),
    create_task_id            STRING OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Create Task Id\", \"clmn_desc\": \"Airflow task ID that created this record\"}"),
    create_task_run_id        STRING OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Create Task Run Id\", \"clmn_desc\": \"Airflow run ID that created this record\"}"),
    update_date               TIMESTAMP OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Update Date\", \"clmn_desc\": \"Timestamp when record was updated in cleaned layer\"}"),
    update_task_id            STRING OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Update Task Id\", \"clmn_desc\": \"Airflow task ID that updated this record\"}"),
    update_task_run_id        STRING OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Update Task Run Id\", \"clmn_desc\": \"Airflow run ID that updated this record\"}")
);