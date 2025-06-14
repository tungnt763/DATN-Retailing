SELECT 
  trn_ext_sale_dol,
  trn_ext_grs_prft_dol,
  trn_ext_dscnt_dol,
  trn_ext_cost_dol,
  SPLIT(trn_id, '_')[OFFSET(0)] AS trn_invoice_id,
  trn_sale_qty,
  trn_type,
  str_name,
  str_sz,
  lct_tr_cntry,
  prd_ctgry,
  prd_sub_ctgry,
  dscnt_desc,
  CASE
    WHEN w.wthr_tmp_avg < 20 THEN '<20°C'
    WHEN w.wthr_tmp_avg BETWEEN 20 AND 25 THEN '20-25°C'
    WHEN w.wthr_tmp_avg BETWEEN 25 AND 30 THEN '25-30°C'
    ELSE '>30°C'
  END AS temperature_bin,
  CASE
    WHEN wthr_prcp = 0 THEN '0'
    WHEN wthr_prcp <= 5 THEN '0-5'
    WHEN wthr_prcp <= 10 THEN '5-10'
    WHEN wthr_prcp <= 20 THEN '10-20'
    ELSE '>20'
  END AS prcp_bucket,
  wthr_tmp_avg AS temp_avg,
  wthr_prcp AS prcp,
  full_date AS date,
  week_in_year AS week,
  calendar_month AS month,
  calendar_month_name AS month_name,
  calendar_year AS year
FROM `datn-retailing.edw.fact_transactions` t
JOIN `datn-retailing.edw.dim_dates` d
  ON t.trn_dt_key = CAST(d.date_key AS STRING)
JOIN `datn-retailing.edw.dim_stores` s
  ON t.trn_str_key = s.str_surr_key
JOIN `datn-retailing.edw.dim_products` p 
  ON t.trn_prd_key = p.prd_surr_key
JOIN `datn-retailing.edw.dim_discounts` di 
  ON t.trn_dscnt_key = di.dscnt_surr_key
JOIN `datn-retailing.edw.dim_locations` l
  ON t.trn_str_lct_key = l.lct_surr_key
JOIN `datn-retailing.edw.dim_weather` w
  ON t.trn_wthr_key = w.wthr_surr_key