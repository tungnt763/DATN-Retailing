WITH base AS (
  SELECT
    s.str_surr_key,
    s.str_name,
    COALESCE(d.calendar_month, -1) AS month,
    COALESCE(d.calendar_year, -1) AS year,
    SUM(CASE WHEN t.trn_type = 'Sale' THEN COALESCE(t.trn_ext_sale_dol, 0) ELSE COALESCE(-t.trn_ext_sale_dol, 0) END) AS store_sales
  FROM `datn-retailing.edw.fact_transactions` t
  JOIN `datn-retailing.edw.dim_stores` s ON s.str_surr_key = t.trn_str_key
  JOIN `datn-retailing.edw.dim_dates` d ON t.trn_dt_key = CAST(d.date_key AS STRING)
  GROUP BY s.str_surr_key, s.str_name, month, year
),
total_by_month AS (
  SELECT
    month,
    year,
    COUNT(*) AS store_count,
    SUM(store_sales) AS total_sales
  FROM base
  GROUP BY month, year
),
final AS (
  SELECT
    b.str_name,
    b.month,
    b.year,
    b.store_sales,
    ROUND((tb.total_sales - b.store_sales) / NULLIF(tb.store_count - 1, 0), 2) AS avg_other_store_sales
  FROM base b
  JOIN total_by_month tb
    ON b.month = tb.month AND b.year = tb.year
)
SELECT * FROM final
ORDER BY year, month