-- Clean & Transform for table: {{ params.table_name }}

INSERT INTO `{{ params.project_name }}.{{ params.output_dataset }}.{{ params.table_name }}`
WITH cleaned_data AS (
SELECT 
    {{ params.cleaned_column_expressions }},
    loaded_batch,
    loaded_part,
    batch_load_ts
FROM `{{ params.project_name }}.{{ params.input_dataset }}.{{ params.table_name }}`
WHERE
    loaded_batch = '{{ execution_date.int_timestamp }}'
    AND loaded_part = DATE(TIMESTAMP_SECONDS(CAST('{{ execution_date.int_timestamp }}' AS INT64)))
),

deduplicated_valid_data AS (
SELECT * EXCEPT(row_num)
FROM (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY {{ params.pk_expr }}) AS row_num
    FROM cleaned_data
)
WHERE row_num = 1
)

SELECT 
    {{ params.selected_columns }},
    CURRENT_TIMESTAMP() AS create_date,
    '{{ task_instance.task_id }}' AS create_task_id,
    '{{ task_instance.run_id }}' AS create_task_run_id
FROM deduplicated_valid_data;
