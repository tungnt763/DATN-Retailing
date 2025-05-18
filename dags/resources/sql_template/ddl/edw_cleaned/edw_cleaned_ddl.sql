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

CREATE TABLE IF NOT EXISTS `datn-retailing.edw_cleaned.employees` (
    emply_id                  STRING NOT NULL OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Employee ID\", \"clmn_desc\": \"Employee ID of the employees table\"}"),
    emply_str_id              STRING  OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Store ID\", \"clmn_desc\": \"Store ID of the employees table\"}"),
    emply_name                STRING  OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Name\", \"clmn_desc\": \"Name of the employees table\"}"),
    emply_pstn                STRING  OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Position\", \"clmn_desc\": \"Position of the employees table\"}"),
    loaded_batch              STRING OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Loaded Batch\", \"clmn_desc\": \"Unix timestamp of the loaded batch\"}"),
    loaded_part               DATE OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Loaded Part\", \"clmn_desc\": \"Partition date of the batch load\"}"),
    batch_load_ts             TIMESTAMP OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Batch Load Ts\", \"clmn_desc\": \"Timestamp when batch was loaded\"}"),
    create_date               TIMESTAMP OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Create Date\", \"clmn_desc\": \"Timestamp when record was created in cleaned layer\"}"),
    create_task_id            STRING OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Create Task Id\", \"clmn_desc\": \"Airflow task ID that created this record\"}"),
    create_task_run_id        STRING OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Create Task Run Id\", \"clmn_desc\": \"Airflow run ID that created this record\"}")
);

CREATE TABLE IF NOT EXISTS `datn-retailing.edw_cleaned.products` (
    prd_id                    STRING NOT NULL OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Product ID\", \"clmn_desc\": \"Product ID of the products table\"}"),
    prd_ctgry                 STRING NOT NULL OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Category\", \"clmn_desc\": \"Category of the products table\"}"),
    prd_sub_ctgry             STRING NOT NULL OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Sub Category\", \"clmn_desc\": \"Sub Category of the products table\"}"),
    prd_desc_pt               STRING  OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Description PT\", \"clmn_desc\": \"Description PT of the products table\"}"),
    prd_desc_de               STRING  OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Description DE\", \"clmn_desc\": \"Description DE of the products table\"}"),
    prd_desc_fr               STRING  OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Description FR\", \"clmn_desc\": \"Description FR of the products table\"}"),
    prd_desc_es               STRING  OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Description ES\", \"clmn_desc\": \"Description ES of the products table\"}"),
    prd_desc_en               STRING  OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Description EN\", \"clmn_desc\": \"Description EN of the products table\"}"),
    prd_desc_zh               STRING  OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Description ZH\", \"clmn_desc\": \"Description ZH of the products table\"}"),
    prd_color                 STRING  OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Color\", \"clmn_desc\": \"Color of the products table\"}"),
    prd_sizes                 STRING  OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Sizes\", \"clmn_desc\": \"Sizes of the products table\"}"),
    prd_cost                  NUMERIC NOT NULL OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Production Cost\", \"clmn_desc\": \"Production Cost of the products table\"}"),
    loaded_batch              STRING OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Loaded Batch\", \"clmn_desc\": \"Unix timestamp of the loaded batch\"}"),
    loaded_part               DATE OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Loaded Part\", \"clmn_desc\": \"Partition date of the batch load\"}"),
    batch_load_ts             TIMESTAMP OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Batch Load Ts\", \"clmn_desc\": \"Timestamp when batch was loaded\"}"),
    create_date               TIMESTAMP OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Create Date\", \"clmn_desc\": \"Timestamp when record was created in cleaned layer\"}"),
    create_task_id            STRING OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Create Task Id\", \"clmn_desc\": \"Airflow task ID that created this record\"}"),
    create_task_run_id        STRING OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Create Task Run Id\", \"clmn_desc\": \"Airflow run ID that created this record\"}")
);

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

CREATE TABLE IF NOT EXISTS `datn-retailing.edw_cleaned.transactions` (
    trn_invc_id               STRING NOT NULL OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Invoice ID\", \"clmn_desc\": \"Invoice ID of the transactions table\"}"),
    trn_line                  INT64 NOT NULL OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Line\", \"clmn_desc\": \"Line of the transactions table\"}"),
    trn_cstmr_id              STRING NOT NULL OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Customer ID\", \"clmn_desc\": \"Customer ID of the transactions table\"}"),
    trn_prd_id                STRING NOT NULL OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Product ID\", \"clmn_desc\": \"Product ID of the transactions table\"}"),
    trn_sz                    STRING  OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Size\", \"clmn_desc\": \"Size of the transactions table\"}"),
    trn_cl                    STRING  OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Color\", \"clmn_desc\": \"Color of the transactions table\"}"),
    trn_unit_prc              NUMERIC  OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Unit Price\", \"clmn_desc\": \"Unit Price of the transactions table\"}"),
    trn_qty                   INT64 NOT NULL OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Quantity\", \"clmn_desc\": \"Quantity of the transactions table\"}"),
    trn_date                  TIMESTAMP NOT NULL OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Date\", \"clmn_desc\": \"Date of the transactions table\"}"),
    trn_dscnt                 NUMERIC NOT NULL OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Discount\", \"clmn_desc\": \"Discount of the transactions table\"}"),
    trn_line_ttl              NUMERIC  OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Line Total\", \"clmn_desc\": \"Line Total of the transactions table\"}"),
    trn_str_id                STRING NOT NULL OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Store ID\", \"clmn_desc\": \"Store ID of the transactions table\"}"),
    trn_emply_id              STRING NOT NULL OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Employee ID\", \"clmn_desc\": \"Employee ID of the transactions table\"}"),
    trn_crncy                 STRING  OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Currency\", \"clmn_desc\": \"Currency of the transactions table\"}"),
    trn_crncy_sbl             STRING  OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Currency Symbol\", \"clmn_desc\": \"Currency Symbol of the transactions table\"}"),
    trn_sku                   STRING  OPTIONS(description="{\"clmn_lgcl_name_eng\": \"SKU\", \"clmn_desc\": \"SKU of the transactions table\"}"),
    trn_type                  STRING  OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Transaction Type\", \"clmn_desc\": \"Transaction Type of the transactions table\"}"),
    trn_pymnt_mthd            STRING  OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Payment Method\", \"clmn_desc\": \"Payment Method of the transactions table\"}"),
    trn_invc_ttl              NUMERIC  OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Invoice Total\", \"clmn_desc\": \"Invoice Total of the transactions table\"}"),
    loaded_batch              STRING OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Loaded Batch\", \"clmn_desc\": \"Unix timestamp of the loaded batch\"}"),
    loaded_part               DATE OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Loaded Part\", \"clmn_desc\": \"Partition date of the batch load\"}"),
    batch_load_ts             TIMESTAMP OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Batch Load Ts\", \"clmn_desc\": \"Timestamp when batch was loaded\"}"),
    create_date               TIMESTAMP OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Create Date\", \"clmn_desc\": \"Timestamp when record was created in cleaned layer\"}"),
    create_task_id            STRING OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Create Task Id\", \"clmn_desc\": \"Airflow task ID that created this record\"}"),
    create_task_run_id        STRING OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Create Task Run Id\", \"clmn_desc\": \"Airflow run ID that created this record\"}")
);

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