DECLARE max_timestamp TIMESTAMP;
SET max_timestamp = TIMESTAMP('{{ task_instance.xcom_pull(task_ids="edw_layer.get_max_timestamp", key="max_timestamp") }}');

IF max_timestamp = TIMESTAMP('1900-01-01 00:00:00') THEN
    TRUNCATE TABLE `{{ params.project_name }}.{{ params.output_dataset }}.{{ params.output_table }}`;
END IF;

INSERT INTO `{{ params.project_name }}.{{ params.output_dataset }}.{{ params.output_table }}_temp`
SELECT
    {{ params.col_names }},
    create_date,
    create_task_id,
    create_task_run_id,
    update_date,
    update_task_id,
    update_task_run_id,
    '0'
FROM `{{ params.project_name }}.{{ params.output_dataset }}.{{ params.output_table }}`;