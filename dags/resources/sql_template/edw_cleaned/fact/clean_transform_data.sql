CREATE TABLE IF NOT EXISTS `{{ params.project_name }}.{{ params.output_dataset }}.{{ params.table_name }}_temp` AS

-- Cast data from input table
WITH casted_table AS (
  SELECT
    -- SAFE_CAST(UPPER(TRIM(trn_invc_id)) AS STRING) AS trn_invc_id,
    -- SAFE_CAST(TRIM(trn_line) AS INT64) AS trn_line,
    -- SAFE_CAST(TRIM(trn_cstmr_id) AS STRING) AS trn_cstmr_id,
    -- SAFE_CAST(TRIM(trn_prd_id) AS STRING) AS trn_prd_id,
    -- SAFE_CAST(UPPER(TRIM(trn_sz)) AS STRING) AS trn_sz,
    -- SAFE_CAST(UPPER(TRIM(trn_cl)) AS STRING) AS trn_cl,
    -- SAFE_CAST(TRIM(trn_unit_prc) AS NUMERIC) AS trn_unit_prc,
    -- SAFE_CAST(TRIM(trn_qty) AS INT64) AS trn_qty,
    -- SAFE_CAST(TRIM(trn_date) AS TIMESTAMP) AS trn_date,
    -- SAFE_CAST(TRIM(trn_dscnt) AS NUMERIC) AS trn_dscnt,
    -- SAFE_CAST(TRIM(trn_line_ttl) AS NUMERIC) AS trn_line_ttl,
    -- SAFE_CAST(TRIM(trn_str_id) AS STRING) AS trn_str_id,
    -- SAFE_CAST(TRIM(trn_emply_id) AS STRING) AS trn_emply_id,
    -- SAFE_CAST(UPPER(TRIM(trn_crncy)) AS STRING) AS trn_crncy,
    -- SAFE_CAST(UPPER(TRIM(trn_crncy_sbl)) AS STRING) AS trn_crncy_sbl,
    -- SAFE_CAST(UPPER(TRIM(trn_sku)) AS STRING) AS trn_sku,
    -- SAFE_CAST(INITCAP(TRIM(trn_type)) AS STRING) AS trn_type,
    -- SAFE_CAST(INITCAP(TRIM(trn_pymnt_mthd)) AS STRING) AS trn_pymnt_mthd,
    -- SAFE_CAST(TRIM(trn_invc_ttl) AS NUMERIC) AS trn_invc_ttl,
    {{ params.cast_exprs }},
    loaded_batch,
    loaded_part,
    batch_load_ts,
    create_date,
    create_task_id,
    create_task_run_id
  FROM
    `{{ params.project_name }}.{{ params.input_dataset }}.{{ params.table_name }}`
  WHERE 
    create_date > TIMESTAMP('{{ task_instance.xcom_pull(task_ids="loading_layer.get_max_timestamp", key="max_timestamp") }}')
),

-- Deduplicate data
deduplicated_table AS (
  SELECT * EXCEPT (row_num)
  FROM (
    SELECT 
      *,
      ROW_NUMBER() OVER (PARTITION BY {{ params.pk_expr }} ORDER BY trn_date DESC, batch_load_ts DESC) AS row_num
    FROM
      casted_table
  )
  WHERE 
    row_num = 1
),

-- Handle null values
handled_null_table AS (
  SELECT
    t.trn_invc_id,
    t.trn_line,
    t.trn_cstmr_id,
    ctm.cstmr_id AS expt_cstmr_id,
    t.trn_prd_id,
    p.prd_id AS expt_prd_id,
    CASE WHEN t.trn_sz IS NULL OR t.trn_sz = '' OR t.trn_sz NOT IN UNNEST(SPLIT(p.prd_sizes, '|')) THEN 'Unknown'
    ELSE t.trn_sz END AS trn_sz,
    CASE WHEN t.trn_cl IS NULL OR t.trn_cl = '' OR t.trn_cl NOT IN UNNEST(SPLIT(p.prd_color, '|')) THEN 'Unknown'
    ELSE t.trn_cl END AS trn_cl,
    t.trn_unit_prc,
    t.trn_qty,
    t.trn_date,
    t.trn_dscnt,
    d.dscnt_value AS expt_dscnt,
    COALESCE(t.trn_line_ttl, 
      CASE WHEN trn_invc_id LIKE 'INV%' THEN t.trn_unit_prc * (1 - t.trn_dscnt) * t.trn_qty
      ELSE -1 * t.trn_unit_prc * (1 - t.trn_dscnt) * t.trn_qty END
    ) AS trn_line_ttl,
    CASE WHEN trn_invc_id LIKE 'INV%' THEN t.trn_unit_prc * (1 - t.trn_dscnt) * t.trn_qty
    ELSE -1 * t.trn_unit_prc * (1 - t.trn_dscnt) * t.trn_qty END AS expt_line_ttl,
    t.trn_str_id,
    s.str_id AS expt_str_id,
    t.trn_emply_id,
    e.emply_id AS expt_emply_id,
    t.trn_crncy,
    crncy.crncy_cd AS expt_crncy,
    COALESCE(t.trn_crncy_sbl, crncy.crncy_sbl, 'Unknown') AS trn_crncy_sbl,
    COALESCE(t.trn_sku, 'Unknown') AS trn_sku,
    COALESCE(t.trn_type, 
      CASE WHEN trn_invc_id LIKE 'INV%' THEN 'Sale'
      ELSE 'Return' END
    ) AS trn_type,
    CASE WHEN trn_invc_id LIKE 'INV%' THEN 'Sale'
    ELSE 'Return' END AS expt_trn_type,
    COALESCE(t.trn_pymnt_mthd, 'Unknown') AS trn_pymnt_mthd,
    COALESCE(t.trn_invc_ttl, 
      SUM(COALESCE(t.trn_line_ttl, 
      CASE WHEN trn_invc_id LIKE 'INV%' THEN t.trn_unit_prc * (1 - t.trn_dscnt) * t.trn_qty
      ELSE -1 * t.trn_unit_prc * (1 - t.trn_dscnt) * t.trn_qty END
      )) OVER (PARTITION BY t.trn_invc_id)) AS trn_invc_ttl,
    SUM(COALESCE(t.trn_line_ttl, 
      CASE WHEN trn_invc_id LIKE 'INV%' THEN t.trn_unit_prc * (1 - t.trn_dscnt) * t.trn_qty
      ELSE -1 * t.trn_unit_prc * (1 - t.trn_dscnt) * t.trn_qty END
      )) OVER (PARTITION BY t.trn_invc_id) AS expt_invc_ttl,
    t.loaded_batch,
    t.loaded_part,
    t.batch_load_ts,
    t.create_date,
    t.create_task_id,
    t.create_task_run_id
  FROM
    deduplicated_table AS t
  LEFT JOIN `datn-retailing.edw.dim_customers` AS ctm
    ON t.trn_cstmr_id = ctm.cstmr_id
    AND ctm.effective_start_date <= t.trn_date
    AND ctm.effective_end_date >= t.trn_date
  LEFT JOIN `datn-retailing.edw.dim_products` AS p
    ON t.trn_prd_id = p.prd_id
    AND p.effective_start_date <= t.trn_date
    AND p.effective_end_date >= t.trn_date
  LEFT JOIN `datn-retailing.edw.dim_stores` AS s
    ON t.trn_str_id = s.str_id
    AND s.effective_start_date <= t.trn_date
    AND s.effective_end_date >= t.trn_date
  LEFT JOIN `datn-retailing.edw.dim_employees` AS e
    ON t.trn_emply_id = e.emply_id
  LEFT JOIN `datn-retailing.edw.dim_currency` AS crncy
    ON t.trn_crncy = crncy.crncy_cd
    AND crncy.effective_start_date <= t.trn_date
    AND crncy.effective_end_date >= t.trn_date
  LEFT JOIN `datn-retailing.edw.dim_discounts` AS d
    ON DATE(t.trn_date) >= d.dscnt_start_date
    AND DATE(t.trn_date) <= d.dscnt_end_date
    AND t.trn_dscnt = d.dscnt_value
    AND ((p.prd_ctgry = d.dscnt_ctgry AND p.prd_sub_ctgry = d.dscnt_sub_ctgry) OR d.dscnt_ctgry = 'Unknown')
    AND d.effective_start_date <= t.trn_date
    AND d.effective_end_date >= t.trn_date
),

-- Validate data
validated_table AS (
  SELECT 
    -- trn_invc_id,
    -- trn_line,
    -- trn_cstmr_id,
    -- trn_prd_id,
    -- trn_sz,
    -- trn_cl,
    -- trn_unit_prc,
    -- trn_qty,
    -- trn_date,
    -- trn_dscnt,
    -- trn_line_ttl,
    -- trn_str_id,
    -- trn_emply_id,
    -- trn_crncy,
    -- trn_crncy_sbl,
    -- trn_sku,
    -- trn_type,
    -- trn_pymnt_mthd,
    -- trn_invc_ttl,
    {{ params.col_names }},
    loaded_batch,
    loaded_part,
    batch_load_ts,
    create_date,
    create_task_id,
    create_task_run_id,
    trn_cstmr_id = expt_cstmr_id AS is_valid_cstmr_id,
    trn_prd_id = expt_prd_id AS is_valid_prd_id,
    trn_dscnt = expt_dscnt AS is_valid_dscnt_id,
    ABS(trn_line_ttl - expt_line_ttl) <= 0.1 AS is_valid_line_ttl,
    trn_str_id = expt_str_id AS is_valid_str_id,
    trn_emply_id = expt_emply_id AS is_valid_emply_id,
    trn_crncy = expt_crncy AS is_valid_crncy,
    trn_type = expt_trn_type AS is_valid_trn_type,
    ABS(trn_invc_ttl - expt_invc_ttl) <= 0.1 AS is_valid_invc_ttl
  FROM handled_null_table
)
SELECT * FROM validated_table;