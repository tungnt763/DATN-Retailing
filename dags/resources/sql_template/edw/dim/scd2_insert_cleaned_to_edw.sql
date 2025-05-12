DECLARE max_timestamp TIMESTAMP;
SET max_timestamp = TIMESTAMP('{{ task_instance.xcom_pull(task_ids="edw_layer.get_max_timestamp", key="max_timestamp") }}');

IF max_timestamp = TIMESTAMP('1900-01-01 00:00:00') THEN
    TRUNCATE TABLE `{{ params.project_name }}.{{ params.output_dataset }}.{{ params.output_table }}`;
END IF;

CREATE TEMP TABLE cleaned_data AS
SELECT
    GENERATE_UUID() AS surr_key,
    {{ params.old_columns }},
    TIMESTAMP('1900-01-01 00:00:00') AS effective_start_date,
    TIMESTAMP('9999-12-31 23:59:59') AS effective_end_date,
    '1' AS flag_active,
    create_date,
    create_task_id,
    create_task_run_id,
    create_date AS update_date,
    create_task_id AS update_task_id,
    create_task_run_id AS update_task_run_id
FROM `{{ params.project_name }}.{{ params.input_dataset }}.{{ params.input_table }}`;

MERGE `{{ params.project_name }}.{{ params.output_dataset }}.{{ params.output_table }}` AS target
USING cleaned_data AS source
ON  
    {{ params.natural_key_expr }}
AND
    target.flag_active = '1'
    AND (
        {{ params.columns_except_natural_key_diff_expr }}
    )
WHEN MATCHED THEN
    UPDATE SET
        effective_end_date = source.effective_start_date - INTERVAL 1 SECOND,
        flag_active = '0',
        update_date = source.create_date,
        update_task_id = source.create_task_id,
        update_task_run_id = source.create_task_run_id;

INSERT INTO `{{ params.project_name }}.{{ params.output_dataset }}.{{ params.output_table }}`
SELECT
    surr_key,
    {{ params.old_columns_except_method }},
    effective_start_date,
    effective_end_date,
    flag_active,
    create_date,
    create_task_id,
    create_task_run_id,
    update_date,
    update_task_id,
    update_task_run_id
FROM cleaned_data;