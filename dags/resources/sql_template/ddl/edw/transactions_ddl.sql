CREATE TABLE IF NOT EXISTS `datn-retailing.edw.fact_transactions` (
    trn_id                    STRING NOT NULL OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Transaction ID\", \"clmn_desc\": \"Transaction ID of the fact_transactions table\"}"),
    trn_hr                    STRING NOT NULL OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Transaction Hour\", \"clmn_desc\": \"Transaction Hour of the fact_transactions table\"}"),
    trn_dt_key                STRING NOT NULL OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Date Key\", \"clmn_desc\": \"Date Key of the fact_transactions table\"}"),
    trn_wthr_key              STRING NOT NULL OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Weather Key\", \"clmn_desc\": \"Weather Key of the fact_transactions table\"}"),
    trn_cstmr_key             STRING NOT NULL OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Customer Key\", \"clmn_desc\": \"Customer Key of the fact_transactions table\"}"),
    trn_prd_key               STRING NOT NULL OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Product Key\", \"clmn_desc\": \"Product Key of the fact_transactions table\"}"),
    trn_str_key               STRING NOT NULL OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Store Key\", \"clmn_desc\": \"Store Key of the fact_transactions table\"}"),
    trn_emply_key             STRING NOT NULL OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Employee Key\", \"clmn_desc\": \"Employee Key of the fact_transactions table\"}"),
    trn_dscnt_key             STRING NOT NULL OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Discount Key\", \"clmn_desc\": \"Discount Key of the fact_transactions table\"}"),
    trn_crncy_key             STRING NOT NULL OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Currency Key\", \"clmn_desc\": \"Currency Key of the fact_transactions table\"}"),
    trn_cstmr_lct_key         STRING NOT NULL OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Customer Location Key\", \"clmn_desc\": \"Customer Location Key of the fact_transactions table\"}"),
    trn_str_lct_key           STRING NOT NULL OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Store Location Key\", \"clmn_desc\": \"Store Location Key of the fact_transactions table\"}"),
    trn_sale_qty              INT64 NOT NULL OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Sale Quantity\", \"clmn_desc\": \"Sale Quantity of the fact_transactions table\"}"),
    trn_reg_unit_prc          NUMERIC NOT NULL OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Regular Unit Price\", \"clmn_desc\": \"Regular Unit Price of the fact_transactions table\"}"),
    trn_dscnt_unit_prc        NUMERIC NOT NULL OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Discount Unit Price \", \"clmn_desc\": \"Discount Unit Price  of the fact_transactions table\"}"),
    trn_net_unit_prc          NUMERIC NOT NULL OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Net Unit Price\", \"clmn_desc\": \"Net Unit Price of the fact_transactions table\"}"),
    trn_ext_sale_dol          NUMERIC NOT NULL OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Extended Sales Dollar Amount\", \"clmn_desc\": \"Extended Sales Dollar Amount of the fact_transactions table\"}"),
    trn_ext_dscnt_dol         NUMERIC NOT NULL OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Extended Discount Dollar Amount\", \"clmn_desc\": \"Extended Discount Dollar Amount of the fact_transactions table\"}"),
    trn_ext_cost_dol          NUMERIC NOT NULL OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Extended Cost Dollar Amount\", \"clmn_desc\": \"Extended Cost Dollar Amount of the fact_transactions table\"}"),
    trn_ext_grs_prft_dol      NUMERIC NOT NULL OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Extended Gross Profit Dollar Amount\", \"clmn_desc\": \"Extended Gross Profit Dollar Amount of the fact_transactions table\"}"),
    trn_line_ttl              NUMERIC NOT NULL OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Line Total\", \"clmn_desc\": \"Line Total of the fact_transactions table\"}"),
    trn_sz                    STRING  OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Size\", \"clmn_desc\": \"Size of the fact_transactions table\"}"),
    trn_cl                    STRING  OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Color\", \"clmn_desc\": \"Color of the fact_transactions table\"}"),
    trn_type                  STRING NOT NULL OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Transaction Type\", \"clmn_desc\": \"Transaction Type of the fact_transactions table\"}"),
    trn_pymnt_mthd            STRING  OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Payment Method\", \"clmn_desc\": \"Payment Method of the fact_transactions table\"}"),
    create_date               TIMESTAMP OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Create Date\", \"clmn_desc\": \"Timestamp when record was created in cleaned layer\"}"),
    create_task_id            STRING OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Create Task Id\", \"clmn_desc\": \"Airflow task ID that created this record\"}"),
    create_task_run_id        STRING OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Create Task Run Id\", \"clmn_desc\": \"Airflow run ID that created this record\"}"),
    update_date               TIMESTAMP OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Update Date\", \"clmn_desc\": \"Timestamp when record was updated in cleaned layer\"}"),
    update_task_id            STRING OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Update Task Id\", \"clmn_desc\": \"Airflow task ID that updated this record\"}"),
    update_task_run_id        STRING OPTIONS(description="{\"clmn_lgcl_name_eng\": \"Update Task Run Id\", \"clmn_desc\": \"Airflow run ID that updated this record\"}")
);