CREATE TABLE IF NOT EXISTS `datn-retailing.edw_cleaned.weather` (
    wthr_date                 DATE NOT NULL OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Weather Date\", \"clmn_desc\": \"Weather Date of the weather table\"}"),
    wthr_city                 STRING  OPTIONS(description="{\"clmn_lgcl_name_eng\": \"City\", \"clmn_desc\": \"City of the weather table\"}"),
    wthr_cntry                STRING  OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Country\", \"clmn_desc\": \"Country of the weather table\"}"),
    wthr_lat                  NUMERIC NOT NULL OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Latitude\", \"clmn_desc\": \"Latitude of the weather table\"}"),
    wthr_lon                  NUMERIC NOT NULL OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Longitude\", \"clmn_desc\": \"Longitude of the weather table\"}"),
    wthr_tmp_avg              NUMERIC  OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Temperature Average\", \"clmn_desc\": \"Temperature Average of the weather table\"}"),
    wthr_prcp                 NUMERIC  OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Precipitation\", \"clmn_desc\": \"Precipitation of the weather table\"}"),
    wthr_rain                 NUMERIC  OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Rain\", \"clmn_desc\": \"Rain of the weather table\"}"),
    wthr_shwrs                NUMERIC  OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Showers\", \"clmn_desc\": \"Showers of the weather table\"}"),
    wthr_snwf                 NUMERIC  OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Snowfall\", \"clmn_desc\": \"Snowfall of the weather table\"}"),
    loaded_batch              STRING OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Loaded Batch\", \"clmn_desc\": \"Unix timestamp of the loaded batch\"}"),
    loaded_part               DATE OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Loaded Part\", \"clmn_desc\": \"Partition date of the batch load\"}"),
    batch_load_ts             TIMESTAMP OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Batch Load Ts\", \"clmn_desc\": \"Timestamp when batch was loaded\"}"),
    create_date               TIMESTAMP OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Create Date\", \"clmn_desc\": \"Timestamp when record was created in cleaned layer\"}"),
    create_task_id            STRING OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Create Task Id\", \"clmn_desc\": \"Airflow task ID that created this record\"}"),
    create_task_run_id        STRING OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Create Task Run Id\", \"clmn_desc\": \"Airflow run ID that created this record\"}")
);