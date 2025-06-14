SELECT 
  COUNT(1) AS total_customers,
  cstmr_gender AS gender,
  -- DATE_DIFF(CURRENT_DATE(), cstmr_brthdy, YEAR) AS age,
  CASE
    WHEN DATE_DIFF(CURRENT_DATE(), cstmr_brthdy, YEAR) < 18 THEN '<18'
    WHEN DATE_DIFF(CURRENT_DATE(), cstmr_brthdy, YEAR) BETWEEN 18 AND 24 THEN '18-24'
    WHEN DATE_DIFF(CURRENT_DATE(), cstmr_brthdy, YEAR) BETWEEN 25 AND 34 THEN '25-34'
    WHEN DATE_DIFF(CURRENT_DATE(), cstmr_brthdy, YEAR) BETWEEN 35 AND 44 THEN '35-44'
    WHEN DATE_DIFF(CURRENT_DATE(), cstmr_brthdy, YEAR) BETWEEN 45 AND 54 THEN '45-54'
    WHEN DATE_DIFF(CURRENT_DATE(), cstmr_brthdy, YEAR) BETWEEN 55 AND 64 THEN '55-64'
    ELSE '65+'
  END AS age_group,
  lct_tr_city AS city,
  lct_tr_cntry AS country,
  EXTRACT(YEAR FROM effective_start_date) AS year,
  EXTRACT(QUARTER FROM effective_start_date) AS quarter,
  EXTRACT(MONTH FROM effective_start_date) AS month,
  EXTRACT(WEEK FROM effective_start_date) AS week
FROM `datn-retailing.edw.dim_customers`
JOIN `datn-retailing.edw.dim_locations`
  ON cstmr_city = lct_raw_city
  AND cstmr_cntry = lct_raw_cntry
WHERE flag_active = '1'
GROUP BY 2, 3, 4, 5, 6, 7, 8, 9
ORDER BY 2, 3, 4, 5, 6, 7, 8, 9, 1;