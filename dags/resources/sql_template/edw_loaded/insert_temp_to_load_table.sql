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
    loaded_batch,
    loaded_part,
    batch_load_ts,
    create_date,
    create_task_id,
    create_task_run_id
FROM 
    `{{ params.project_name }}.{{ params.dataset_name }}.{{ params.table_name }}_temp`;