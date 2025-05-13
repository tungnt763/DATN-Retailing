TRUNCATE TABLE `{{ params.project_name }}.{{ params.output_dataset }}.{{ params.table_name }}`;

INSERT INTO `{{ params.project_name }}.{{ params.output_dataset }}.{{ params.table_name }}`
SELECT 
    {{ params.col_names }},
    loaded_batch,
    loaded_part,
    batch_load_ts,
    CURRENT_TIMESTAMP() AS create_date,
    '{{ task_instance.task_id }}' AS create_task_id,
    '{{ task_instance.run_id }}' AS create_task_run_id
FROM 
    `{{ params.project_name }}.{{ params.output_dataset }}.{{ params.table_name }}_temp`
WHERE
    is_valid_cstmr_id = true
    AND is_valid_prd_id = true
    AND is_valid_dscnt_id = true
    AND is_valid_line_ttl = true
    AND is_valid_str_id = true
    AND is_valid_emply_id = true
    AND is_valid_crncy = true
    AND is_valid_trn_type = true
    AND is_valid_invc_ttl = true;