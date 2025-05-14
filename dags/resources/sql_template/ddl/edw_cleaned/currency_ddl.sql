CREATE TABLE IF NOT EXISTS `datn-retailing.edw_cleaned.currency` (
    crncy_cd                  STRING NOT NULL OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Currency Code\", \"clmn_desc\": \"Currency Code of the currency table\"}"),
    crncy_name                STRING  OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Currency Name\", \"clmn_desc\": \"Currency Name of the currency table\"}"),
    crncy_sbl                 STRING  OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Currency Symbol\", \"clmn_desc\": \"Currency Symbol of the currency table\"}"),
    crncy_rt_usd              NUMERIC NOT NULL OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Rate to USD\", \"clmn_desc\": \"Rate to USD of the currency table\"}"),
    crncy_base                STRING NOT NULL OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Base Currency\", \"clmn_desc\": \"Base Currency of the currency table\"}"),
    loaded_batch              STRING OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Loaded Batch\", \"clmn_desc\": \"Unix timestamp of the loaded batch\"}"),
    loaded_part               DATE OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Loaded Part\", \"clmn_desc\": \"Partition date of the batch load\"}"),
    batch_load_ts             TIMESTAMP OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Batch Load Ts\", \"clmn_desc\": \"Timestamp when batch was loaded\"}"),
    create_date               TIMESTAMP OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Create Date\", \"clmn_desc\": \"Timestamp when record was created in cleaned layer\"}"),
    create_task_id            STRING OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Create Task Id\", \"clmn_desc\": \"Airflow task ID that created this record\"}"),
    create_task_run_id        STRING OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Create Task Run Id\", \"clmn_desc\": \"Airflow run ID that created this record\"}")
);