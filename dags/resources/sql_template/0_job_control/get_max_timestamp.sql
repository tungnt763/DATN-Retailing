SELECT 
    COALESCE(MAX(insert_date), TIMESTAMP('1900-01-01 00:00:00.000000')) AS max_timestamp
FROM 
    `datn-retailing.edw.job_log`
WHERE 
    dataset_name = '{_dataset_name}' AND 
    table_name = '{_table_name}';