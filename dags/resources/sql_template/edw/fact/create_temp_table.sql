CREATE TABLE IF NOT EXISTS `{{ params.project_name }}.{{ params.output_dataset }}.{{ params.output_table }}_temp` (
    {{ params.schema_columns }},
    create_date TIMESTAMP,
    create_task_id STRING,
    create_task_run_id STRING,
    update_date TIMESTAMP,
    update_task_id STRING,
    update_task_run_id STRING,
    flg_if STRING,
)
