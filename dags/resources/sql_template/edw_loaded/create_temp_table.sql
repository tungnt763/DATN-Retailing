CREATE TABLE IF NOT EXISTS `{{ params.project_name }}.{{ params.dataset_name }}.{{ params.table_name }}_temp` (
    {{ params.schema_columns }},
    loaded_batch STRING,
    loaded_part DATE,
    batch_load_ts TIMESTAMP
);