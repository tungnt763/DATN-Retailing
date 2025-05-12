TRUNCATE TABLE `{{ params.project_name }}.{{ params.output_dataset }}.{{ params.table_name }}`;

CREATE TEMP TABLE clear_data AS
-- Cast data from input table
WITH casted_data AS (
    SELECT 
        UPPER(TRIM(trn_invc_id)) AS trn_invc_id,
        SAFE_CAST(TRIM(trn_line) AS INT64) AS trn_line,
        TRIM(trn_cstmr_id) AS trn_cstmr_id,
        TRIM(trn_prd_id) AS trn_prd_id,
        TRIM(trn_sz) AS trn_sz,
        TRIM(trn_cl) AS trn_cl,
        SAFE_CAST(TRIM(trn_unit_prc) AS NUMERIC) AS trn_unit_prc,
        SAFE_CAST(TRIM(trn_qty) AS INT64) AS trn_qty,
        TIMESTAMP(TRIM(trn_date)) AS trn_date,
        SAFE_CAST(TRIM(trn_dscnt) AS NUMERIC) AS trn_dscnt,
        SAFE_CAST(TRIM(trn_line_ttl) AS NUMERIC) AS trn_line_ttl,
        TRIM(trn_str_id) AS trn_str_id,
        TRIM(trn_emply_id) AS trn_emply_id,
        TRIM(trn_crncy) AS trn_crncy,
        TRIM(trn_crncy_sbl) AS trn_crncy_sbl,
        TRIM(trn_sku) AS trn_sku,
        INITCAP(TRIM(trn_type)) AS trn_type,
        INITCAP(TRIM(trn_pymnt_mthd)) AS trn_pymnt_mthd,
        SAFE_CAST(TRIM(trn_invc_ttl) AS NUMERIC) AS trn_invc_ttl
    FROM `datn-retailing.edw_loaded.transactions`
),
cleaned_data AS (
    SELECT 
        trn.trn_invc_id,
        trn.trn_line,
        trn.trn_cstmr_id,
        trn.trn_prd_id,
        CASE 
        WHEN trn.trn_sz IS NULL OR trn.trn_sz = '' OR trn.trn_sz NOT IN UNNEST(SPLIT(prd.prd_sizes, '|')) THEN 'Unknown'
        ELSE trn.trn_sz 
        END AS trn_sz,
        CASE 
        WHEN trn.trn_cl IS NULL OR trn.trn_cl = '' OR trn.trn_cl NOT IN UNNEST(SPLIT(prd.prd_color, '|')) THEN 'Unknown'
        ELSE trn.trn_cl 
        END AS trn_cl,
        COALESCE(trn.trn_unit_prc, prd.prd_cost) AS trn_unit_prc,
        trn.trn_qty,
        trn.trn_date,
        COALESCE(trn.trn_dscnt, dscnt.dscnt_value, 0.0) AS trn_dscnt,
        COALESCE(trn.trn_line_ttl, 
            CASE 
            WHEN trn.trn_invc_id LIKE 'INV%' 
            THEN prd.prd_cost * (1 - COALESCE(trn.trn_dscnt, dscnt.dscnt_value, 0.0)) * trn.trn_qty
            ELSE -1 * prd.prd_cost * (1 - COALESCE(trn.trn_dscnt, dscnt.dscnt_value, 0.0)) * trn.trn_qty
            END) AS trn_line_ttl,
        trn.trn_str_id,
        trn.trn_emply_id,
        CASE 
        WHEN trn.trn_crncy IS NULL OR trn.trn_crncy = '' THEN 'Unknown' ELSE trn.trn_crncy 
        END AS trn_crncy,
        CASE 
        WHEN trn.trn_crncy_sbl IS NULL OR trn.trn_crncy_sbl = '' THEN 'Unknown' ELSE trn.trn_crncy_sbl 
        END AS trn_crncy_sbl,
        CASE 
        WHEN trn.trn_sku IS NULL OR trn.trn_sku = '' THEN 'Unknown' ELSE trn.trn_sku 
        END AS trn_sku,
        CASE 
        WHEN trn.trn_type IS NULL OR trn.trn_type = '' 
        THEN CASE WHEN trn.trn_invc_id LIKE 'INV%' THEN 'Sale' ELSE 'Return' END
        ELSE trn.trn_type 
        END AS trn_type,
        CASE 
        WHEN trn.trn_pymnt_mthd IS NULL OR trn.trn_pymnt_mthd = '' THEN 'Unknown' ELSE trn.trn_pymnt_mthd 
        END AS trn_pymnt_mthd,
        CASE 
        WHEN trn.trn_invc_id LIKE 'INV%' THEN 1 ELSE -1 
        END AS is_sale,
        trn.trn_invc_ttl
    FROM casted_data AS trn
    INNER JOIN `datn-retailing.edw.dim_customers` AS cstmr
        ON trn.trn_cstmr_id = cstmr.cstmr_id
        AND cstmr.effective_start_date <= trn.trn_date
        AND cstmr.effective_end_date >= trn.trn_date
    INNER JOIN `datn-retailing.edw.dim_products` AS prd
        ON trn.trn_prd_id = prd.prd_id
        AND prd.effective_start_date <= trn.trn_date
        AND prd.effective_end_date >= trn.trn_date
    INNER JOIN `datn-retailing.edw.dim_stores` AS str
        ON trn.trn_str_id = str.str_id
        AND str.effective_start_date <= trn.trn_date
        AND str.effective_end_date >= trn.trn_date
    INNER JOIN `datn-retailing.edw.dim_employees` AS emply
        ON trn.trn_emply_id = emply.emply_id
    LEFT JOIN `datn-retailing.edw.dim_discounts` AS dscnt
        ON dscnt.dscnt_start_date <= DATE(trn.trn_date)
        AND dscnt.dscnt_end_date >= DATE(trn.trn_date)
        AND (dscnt.dscnt_ctgry = 'Unknown' OR dscnt.dscnt_ctgry = prd.prd_ctgry)
        AND (dscnt.dscnt_sub_ctgry = 'Unknown' OR dscnt.dscnt_sub_ctgry = prd.prd_sub_ctgry)
        AND dscnt.effective_start_date <= trn.trn_date
        AND dscnt.effective_end_date >= trn.trn_date
),
-- Validate data
validated_data AS (
    SELECT 
        *,
        SUM(trn.trn_line_ttl) OVER (PARTITION BY trn.trn_invc_id) AS trn_expt_invc_ttl,
        CASE 
        WHEN trn.trn_dscnt = COALESCE(dscnt.dscnt_value, 0.0) THEN TRUE
        ELSE FALSE
        END AS is_valid_trn_dscnt,
        CASE 
        WHEN ABS(trn.trn_unit_prc - prd.prd_unit_prc) > 0.01 THEN FALSE
        ELSE TRUE
        END AS is_valid_trn_unit_prc,
        CASE 
        WHEN ABS(trn.trn_line_ttl - is_sale * prd.prd_unit_prc * (1 - COALESCE(trn.trn_dscnt, dscnt.dscnt_value, 0.0)) * trn.trn_qty) > 0.01 THEN FALSE
        ELSE TRUE
        END AS is_valid_trn_line_ttl,
        CASE
        WHEN trn.trn_type = 'Sale' AND is_sale = -1 THEN FALSE
        WHEN trn.trn_type = 'Return' AND is_sale = 1 THEN FALSE
        ELSE TRUE
        END AS is_valid_trn_type,
        CASE
        WHEN ABS(trn.trn_invc_ttl - trn_expt_invc_ttl) > 0.01 THEN FALSE
        ELSE TRUE
        END AS is_valid_trn_invc_ttl
    FROM cleaned_data AS trn
    INNER JOIN `datn-retailing.edw.dim_products` AS prd
        ON trn.trn_prd_id = prd.prd_id
        AND prd.effective_start_date <= trn.trn_date
        AND prd.effective_end_date >= trn.trn_date
    INNER JOIN `datn-retailing.edw.dim_stores` AS str
        ON trn.trn_str_id = str.str_id
        AND str.effective_start_date <= trn.trn_date
        AND str.effective_end_date >= trn.trn_date
    INNER JOIN `datn-retailing.edw.dim_employees` AS emply
        ON trn.trn_emply_id = emply.emply_id
    LEFT JOIN `datn-retailing.edw.dim_discounts` AS dscnt
        ON dscnt.dscnt_start_date <= DATE(trn.trn_date)
        AND dscnt.dscnt_end_date >= DATE(trn.trn_date)
        AND (dscnt.dscnt_ctgry = 'Unknown' OR dscnt.dscnt_ctgry = prd.prd_ctgry)
        AND (dscnt.dscnt_sub_ctgry = 'Unknown' OR dscnt.dscnt_sub_ctgry = prd.prd_sub_ctgry)
        AND dscnt.effective_start_date <= trn.trn_date
        AND dscnt.effective_end_date >= trn.trn_date
)

INSERT INTO `{{ params.project_name }}.{{ params.output_dataset }}.{{ params.table_name }}`
