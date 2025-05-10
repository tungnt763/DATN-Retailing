LOAD DATA INTO `datn-retailing.edw.dim_dates`
FROM FILES (
  format = 'CSV',
  uris = ['gs://retailing_data/raw/dates/date.csv'],
  skip_leading_rows = 1
);