CREATE TABLE IF NOT EXISTS `datn-retailing.edw.dim_currency` (
    crncy_surr_key            STRING NOT NULL OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Currency Surrogate Key\", \"clmn_desc\": \"Currency Surrogate Key of the dim_currency table\"}"),
    crncy_cd                  STRING NOT NULL OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Currency Code\", \"clmn_desc\": \"Currency Code of the dim_currency table\"}"),
    crncy_name                STRING  OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Currency Name\", \"clmn_desc\": \"Currency Name of the dim_currency table\"}"),
    crncy_sbl                 STRING  OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Currency Symbol\", \"clmn_desc\": \"Currency Symbol of the dim_currency table\"}"),
    crncy_rt_usd              NUMERIC NOT NULL OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Rate to USD\", \"clmn_desc\": \"Rate to USD of the dim_currency table\"}"),
    crncy_rt_dt               DATE NOT NULL OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Rate Date\", \"clmn_desc\": \"Rate Date of the dim_currency table\"}"),
    crncy_base                STRING NOT NULL OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Base Currency\", \"clmn_desc\": \"Base Currency of the dim_currency table\"}"),
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