WITH transactions_with_invoice AS (
  SELECT
    trn_cstmr_key AS cstmr_id,
    trn_ext_sale_dol * crncy_rt_usd AS sale,
    SPLIT(trn_id, '_')[OFFSET(0)] AS invoice_id,
    PARSE_DATE('%Y%m%d', CAST(trn_dt_key AS STRING)) AS trn_date,
    week_in_year AS week,
    calendar_month AS month,
    calendar_year AS year
  FROM `datn-retailing.edw.fact_transactions`
  JOIN `datn-retailing.edw.dim_dates`
    ON trn_dt_key = CAST(date_key AS STRING)
  JOIN `datn-retailing.edw.dim_currency`
    ON trn_crncy_key = crncy_surr_key
),
orders_per_customer AS (
  SELECT
    cstmr_id,
    week,
    month,
    year,
    sale,
    invoice_id,
    COUNT(DISTINCT invoice_id) OVER (PARTITION BY cstmr_id) AS order_count,
    MAX(trn_date) OVER (PARTITION BY cstmr_id) AS last_order_date 
  FROM transactions_with_invoice
)
SELECT
  c.cstmr_surr_key,
  c.cstmr_gender,
  COALESCE(l.lct_tr_city, 'Unknown') AS city,
  COALESCE(l.lct_tr_cntry, 'Unknown') AS country,
  CASE
    WHEN o.last_order_date IS NULL THEN 'No Order'
    WHEN DATE_DIFF(DATE('2024-01-01'), o.last_order_date, MONTH) > 12 THEN 'Sleeper'
    WHEN o.order_count = 1 THEN 'New Buyer'
    WHEN o.order_count BETWEEN 2 AND 4 THEN 'Returning 1'
    WHEN o.order_count BETWEEN 5 AND 10 THEN 'Returning 2'
    WHEN o.order_count > 10 THEN 'Top Buyer'
    ELSE 'Other'
  END AS segment,
  COALESCE(o.week, -1) AS week,
  COALESCE(o.month, -1) AS month,
  COALESCE(o.year, -1) AS year,
  COALESCE(o.sale, 0) AS sale,
  COALESCE(o.invoice_id, 'Unknown') AS invoice_id
FROM `datn-retailing.edw.dim_customers` c
LEFT JOIN orders_per_customer o
  ON c.cstmr_surr_key = o.cstmr_id
LEFT JOIN `datn-retailing.edw.dim_locations` l
  ON c.cstmr_city = l.lct_raw_city
  AND c.cstmr_cntry = l.lct_raw_cntry
WHERE c.flag_active = '1'