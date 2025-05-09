-- Clean & Transform for table: {{ params.table_name }}

TRUNCATE TABLE `{{ params.project_name }}.{{ params.output_dataset }}.{{ params.table_name }}`;

INSERT INTO `{{ params.project_name }}.{{ params.output_dataset }}.{{ params.table_name }}`
WITH cleaned_data AS (
    SELECT 
        {{ params.cleaned_column_expressions }},
        loaded_batch,
        loaded_part,
        batch_load_ts
    FROM `{{ params.project_name }}.{{ params.input_dataset }}.{{ params.table_name }}`
    WHERE
        create_date > TIMESTAMP('{{ task_instance.xcom_pull(task_ids="loading_layer.get_max_timestamp", key="max_timestamp") }}')
),

deduplicated_valid_data AS (
    SELECT 
        {{ params.columns }},
        loaded_batch,
        loaded_part,
        batch_load_ts,
        ROW_NUMBER() OVER (PARTITION BY {{ params.pk_expr }} ORDER BY batch_load_ts DESC) AS row_num
    FROM cleaned_data
)

SELECT 
    {{ params.selected_columns }},
    CURRENT_TIMESTAMP() AS create_date,
    '{{ task_instance.task_id }}' AS create_task_id,
    '{{ task_instance.run_id }}' AS create_task_run_id
FROM deduplicated_valid_data
WHERE 
    row_num = 1;
