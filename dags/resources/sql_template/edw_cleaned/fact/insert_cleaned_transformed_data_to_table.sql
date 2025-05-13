INSERT INTO `{{ params.project_name }}.{{ params.output_dataset }}.{{ params.table_name }}`
SELECT 
    {{ params.columns }},
    loaded_batch,
    loaded_part,
    batch_load_ts,
    CURRENT_TIMESTAMP() AS create_date,
    '{{ task_instance.task_id }}' AS create_task_id,
    '{{ task_instance.run_id }}' AS create_task_run_id
FROM 
    `{{ params.project_name }}.{{ params.output_dataset }}.{{ params.table_name }}_temp`;
WHERE
    is_valid_cstmr_id
    AND is_valid_prd_id
    AND is_valid_dscnt_id
    AND is_valid_line_ttl
    AND is_valid_str_id
    AND is_valid_emply_id
    AND is_valid_crncy
    AND is_valid_trn_type
    AND is_valid_invc_ttl;