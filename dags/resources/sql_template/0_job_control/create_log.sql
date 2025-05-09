CREATE OR REPLACE TABLE `datn-retailing.edw.job_log` 
(
    task_id STRING NOT NULL,
    dataset_name STRING NOT NULL,
    table_name STRING NOT NULL,
    run_date DATE NOT NULL,
    insert_date TIMESTAMP NOT NULL 
)
