CREATE TABLE IF NOT EXISTS `datn-retailing.edw.dim_employees` (
    emply_surr_key            STRING NOT NULL OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Employee Surrogate Key\", \"clmn_desc\": \"Employee Surrogate Key of the dim_employees table\"}"),
    emply_id                  STRING NOT NULL OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Employee ID\", \"clmn_desc\": \"Employee ID of the dim_employees table\"}"),
    emply_name                STRING  OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Name\", \"clmn_desc\": \"Name of the dim_employees table\"}"),
    emply_pstn                STRING  OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Position\", \"clmn_desc\": \"Position of the dim_employees table\"}"),
    create_date               TIMESTAMP OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Create Date\", \"clmn_desc\": \"Timestamp when record was created in cleaned layer\"}"),
    create_task_id            STRING OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Create Task Id\", \"clmn_desc\": \"Airflow task ID that created this record\"}"),
    create_task_run_id        STRING OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Create Task Run Id\", \"clmn_desc\": \"Airflow run ID that created this record\"}"),
    update_date               TIMESTAMP OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Update Date\", \"clmn_desc\": \"Timestamp when record was updated in cleaned layer\"}"),
    update_task_id            STRING OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Update Task Id\", \"clmn_desc\": \"Airflow task ID that updated this record\"}"),
    update_task_run_id        STRING OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Update Task Run Id\", \"clmn_desc\": \"Airflow run ID that updated this record\"}")
);