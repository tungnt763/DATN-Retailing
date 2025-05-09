INSERT INTO `{{ params.project_name }}.{{ params.dataset_name }}.{{ params.table_name }}_temp`
SELECT 
    {{ params.columns }},
    '{{ params.loaded_batch }}' AS loaded_batch,
    DATE(TIMESTAMP_SECONDS(CAST('{{ params.loaded_batch }}' AS INT64))) AS loaded_part,
    TIMESTAMP_SECONDS(CAST('{{ params.loaded_batch }}' AS INT64)) AS batch_load_ts
FROM `{{ params.project_name }}.{{ params.dataset_name }}.{{ params.table_name }}_staging` s