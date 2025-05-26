-- Customer segments by Gender

WITH transactions_with_invoice AS (
  SELECT
    trn_cstmr_key,
    SPLIT(trn_id, '_')[OFFSET(0)] AS invoice_id,
    PARSE_DATE('%Y%m%d', CAST(trn_dt_key AS STRING)) AS trn_date
  FROM `datn-retailing.edw.fact_transactions`
),
orders_per_customer AS (
  SELECT
    trn_cstmr_key,
    COUNT(DISTINCT invoice_id) AS order_count,
    MAX(trn_date) AS last_order_date
  FROM transactions_with_invoice
  GROUP BY trn_cstmr_key
),
customer_segment AS (
  SELECT
    c.cstmr_surr_key,
    c.cstmr_gender,
    IFNULL(o.order_count, 0) AS order_count,
    o.last_order_date,
    CURRENT_DATE() AS today,
    CASE
      WHEN o.last_order_date IS NULL THEN 'Sleeper'
      WHEN DATE_DIFF(CURRENT_DATE(), o.last_order_date, MONTH) > 12 THEN 'Sleeper'
      WHEN o.order_count = 1 THEN 'New Buyer'
      WHEN o.order_count BETWEEN 2 AND 4 THEN 'Returning 1'
      WHEN o.order_count BETWEEN 5 AND 10 THEN 'Returning 2'
      WHEN o.order_count > 10 THEN 'Top Buyer'
      ELSE 'Other'
    END AS segment
  FROM `datn-retailing.edw.dim_customers` c
  LEFT JOIN orders_per_customer o
    ON c.cstmr_surr_key = o.trn_cstmr_key
)
SELECT
  cstmr_surr_key,
  cstmr_gender,
  segment
FROM customer_segment;

-- Sales by Segment

WITH transactions_with_invoice AS (
  SELECT
    t.trn_cstmr_key,
    t.trn_ext_sale_dol * c.crncy_rt_usd AS cstmr_sale_amount,
    SPLIT(t.trn_id, '_')[OFFSET(0)] AS invoice_id,
    PARSE_DATE('%Y%m%d', CAST(t.trn_dt_key AS STRING)) AS trn_date
  FROM `datn-retailing.edw.fact_transactions` AS t
  JOIN `datn-retailing.edw.dim_currency` AS c
    ON t.trn_crncy_key = c.crncy_surr_key
),
orders_sale_per_customer AS (
  SELECT
    trn_cstmr_key,
    SUM(cstmr_sale_amount) AS cstmr_sale_amount,
    COUNT(DISTINCT invoice_id) AS order_count,
    MAX(trn_date) AS last_order_date
  FROM transactions_with_invoice
  GROUP BY trn_cstmr_key
),
customer_segment AS (
  SELECT
    c.cstmr_surr_key,
    COALESCE(o.cstmr_sale_amount, 0) AS cstmr_sale_amount,
    CASE
      WHEN o.last_order_date IS NULL THEN 'Sleeper'
      WHEN DATE_DIFF(CURRENT_DATE(), o.last_order_date, MONTH) > 12 THEN 'Sleeper'
      WHEN o.order_count = 1 THEN 'New Buyer'
      WHEN o.order_count BETWEEN 2 AND 4 THEN 'Returning 1'
      WHEN o.order_count BETWEEN 5 AND 10 THEN 'Returning 2'
      WHEN o.order_count > 10 THEN 'Top Buyer'
      ELSE 'Other'
    END AS segment
  FROM `datn-retailing.edw.dim_customers` c
  LEFT JOIN orders_sale_per_customer o
    ON c.cstmr_surr_key = o.trn_cstmr_key
)
SELECT
  cstmr_surr_key,
  cstmr_sale_amount,
  segment
FROM customer_segment;

-- Segment distribution by State
WITH transactions_with_invoice AS (
  SELECT
    trn_cstmr_key,
    SPLIT(trn_id, '_')[OFFSET(0)] AS invoice_id,
    PARSE_DATE('%Y%m%d', CAST(trn_dt_key AS STRING)) AS trn_date
  FROM `datn-retailing.edw.fact_transactions`
),
orders_sale_per_customer AS (
  SELECT
    trn_cstmr_key,
    COUNT(DISTINCT invoice_id) AS order_count,
    MAX(trn_date) AS last_order_date
  FROM transactions_with_invoice
  GROUP BY trn_cstmr_key
),
customer_segment AS (
  SELECT
    c.cstmr_surr_key,
    c.cstmr_city,
    c.cstmr_cntry,
    CASE
      WHEN o.last_order_date IS NULL THEN 'Sleeper'
      WHEN DATE_DIFF(CURRENT_DATE(), o.last_order_date, MONTH) > 12 THEN 'Sleeper'
      WHEN o.order_count = 1 THEN 'New Buyer'
      WHEN o.order_count BETWEEN 2 AND 4 THEN 'Returning 1'
      WHEN o.order_count BETWEEN 5 AND 10 THEN 'Returning 2'
      WHEN o.order_count > 10 THEN 'Top Buyer'
      ELSE 'Other'
    END AS segment
  FROM `datn-retailing.edw.dim_customers` c
  LEFT JOIN orders_sale_per_customer o
    ON c.cstmr_surr_key = o.trn_cstmr_key
)
SELECT
  cstmr_cntry,
  cstmr_city,
  segment,
  COUNT(cstmr_surr_key) AS segment_count,
  COUNT(cstmr_surr_key) * 1.0 / SUM(COUNT(cstmr_surr_key)) OVER (PARTITION BY cstmr_cntry, cstmr_city) AS segment_ratio
FROM customer_segment
GROUP BY cstmr_cntry, cstmr_city, segment;