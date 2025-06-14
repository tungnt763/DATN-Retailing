DECLARE max_ts TIMESTAMP;
SET max_ts = TIMESTAMP('{{ task_instance.xcom_pull(task_ids="loading_layer.get_max_timestamp", key="max_timestamp") }}');

-- Tạo bảng nếu chưa tồn tại
CREATE TABLE IF NOT EXISTS `{{ params.project_name }}.{{ params.dataset_name }}.{{ params.table_name }}` (
    {{ params.schema_columns }},
    loaded_batch STRING,
    loaded_part DATE,
    batch_load_ts TIMESTAMP,
    create_date TIMESTAMP,
    create_task_id STRING,
    create_task_run_id STRING
);

-- Xóa dữ liệu nếu max_timestamp là giá trị mặc định
IF max_ts = TIMESTAMP('1900-01-01 00:00:00') THEN
    TRUNCATE TABLE `{{ params.project_name }}.{{ params.dataset_name }}.{{ params.table_name }}`;
END IF;

-- Chèn dữ liệu mới
INSERT INTO `{{ params.project_name }}.{{ params.dataset_name }}.{{ params.table_name }}`
SELECT 
    {{ params.columns }},
    '{{ task_instance.dag_run.conf.loaded_batch }}' AS loaded_batch,
    DATE(TIMESTAMP_SECONDS(CAST('{{ task_instance.dag_run.conf.loaded_batch }}' AS INT64))) AS loaded_part,
    TIMESTAMP_SECONDS(CAST('{{ task_instance.dag_run.conf.loaded_batch }}' AS INT64)) AS batch_load_ts,
    CURRENT_TIMESTAMP() AS create_date,
    '{{ task_instance.task_id }}' AS create_task_id,
    '{{ task_instance.run_id }}' AS create_task_run_id
FROM 
    `{{ params.project_name }}.{{ params.dataset_name }}.{{ params.table_name }}_temp_{{ task_instance.dag_run.conf.loaded_batch }}`;