SELECT
  s.str_surr_key,
  s.str_name,
  l.lct_tr_cntry AS str_cntry,
  c.cstmr_id AS str_cstmr,
  COALESCE(d.full_date, DATE('1900-01-01')) AS date,
  COALESCE(d.week_in_year, -1) AS week,
  COALESCE(d.calendar_month, -1) AS month,
  COALESCE(d.calendar_month_name, 'Unknown') AS month_name,
  COALESCE(d.calendar_year, -1) AS year,
  SPLIT(t.trn_id, '_')[OFFSET(0)] AS order_id,
  trn_type AS order_type,
  CASE WHEN t.trn_type = 'Sale' THEN COALESCE(t.trn_ext_sale_dol, 0) ELSE COALESCE(-t.trn_ext_sale_dol, 0) END AS sale,
  CASE WHEN t.trn_type = 'Sale' THEN COALESCE(t.trn_ext_grs_prft_dol, 0) ELSE COALESCE(-t.trn_ext_grs_prft_dol, 0) END AS profit,
FROM 
  `datn-retailing.edw.dim_stores` s
LEFT JOIN `datn-retailing.edw.fact_transactions` t
  ON s.str_surr_key = t.trn_str_key
LEFT JOIN `datn-retailing.edw.dim_dates` d
  ON t.trn_dt_key = SAFE_CAST(d.date_key AS STRING)
LEFT JOIN `datn-retailing.edw.dim_locations` l
  ON l.lct_raw_cntry = s.str_cntry
  AND l.lct_raw_city = s.str_city
LEFT JOIN `datn-retailing.edw.dim_customers` c
  ON t.trn_cstmr_key = c.cstmr_surr_key