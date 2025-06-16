DECLARE max_timestamp TIMESTAMP;
SET max_timestamp = TIMESTAMP('{{ task_instance.xcom_pull(task_ids="edw_layer.get_max_timestamp", key="max_timestamp") }}');

IF max_timestamp = TIMESTAMP('1900-01-01 00:00:00') THEN
    TRUNCATE TABLE `{{ params.project_name }}.{{ params.output_dataset }}.{{ params.output_table }}`;
END IF;

CREATE TEMP TABLE cleaned_data AS
SELECT
    GENERATE_UUID() AS surr_key,
    {{ params.old_columns }},
    create_date,
    create_task_id,
    create_task_run_id,
    create_date AS update_date,
    create_task_id AS update_task_id,
    create_task_run_id AS update_task_run_id
FROM `{{ params.project_name }}.{{ params.input_dataset }}.{{ params.input_table }}` target;

MERGE `{{ params.project_name }}.{{ params.output_dataset }}.{{ params.output_table }}` AS target
USING cleaned_data AS source
ON  
    {{ params.natural_key_expr }}
WHEN MATCHED THEN
    UPDATE SET
        {{ params.columns_except_natural_key_expr }},
        update_date = CURRENT_TIMESTAMP(),
        update_task_id = '{{ task_instance.task_id }}',
        update_task_run_id = '{{ task_instance.run_id }}'
WHEN NOT MATCHED THEN
    INSERT ({{ params.new_columns }}, create_date, create_task_id, create_task_run_id, update_date, update_task_id, update_task_run_id)
    VALUES (source.surr_key, {{ params.old_columns_in_row }}, CURRENT_TIMESTAMP(), '{{ task_instance.task_id }}', '{{ task_instance.run_id }}', CURRENT_TIMESTAMP(), '{{ task_instance.task_id }}', '{{ task_instance.run_id }}');
