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