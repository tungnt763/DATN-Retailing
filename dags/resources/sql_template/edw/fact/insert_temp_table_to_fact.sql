TRUNCATE TABLE `{{ params.project_name }}.{{ params.output_dataset }}.{{ params.output_table }}`;

INSERT INTO `{{ params.project_name }}.{{ params.output_dataset }}.{{ params.output_table }}`
SELECT
    {{ params.pk_expr }},
    {{ params.array_agg_exprs }},
    CASE ARRAY_AGG(flg_if ORDER BY flg_if ASC)[ORDINAL(1)]
        WHEN "1" THEN CURRENT_TIMESTAMP()
        WHEN "0" THEN ARRAY_AGG(create_date ORDER BY flg_if ASC)[ORDINAL(1)]
    END AS create_date,
    CASE ARRAY_AGG(flg_if ORDER BY flg_if ASC)[ORDINAL(1)]
        WHEN "1" THEN '{{ task_instance.task_id }}'
        WHEN "0" THEN ARRAY_AGG(create_task_id ORDER BY flg_if ASC)[ORDINAL(1)]
    END AS create_task_id,
    CASE ARRAY_AGG(flg_if ORDER BY flg_if ASC)[ORDINAL(1)]
        WHEN "1" THEN '{{ task_instance.run_id }}'
        WHEN "0" THEN ARRAY_AGG(create_task_run_id ORDER BY flg_if ASC)[ORDINAL(1)]
    END AS create_task_run_id,
    CASE ARRAY_AGG(flg_if ORDER BY trn_dt_key DESC, trn_hr DESC, flg_if DESC)[ORDINAL(1)]
        WHEN "1" THEN CURRENT_TIMESTAMP()
        WHEN "0" THEN ARRAY_AGG(update_date ORDER BY trn_dt_key DESC, trn_hr DESC, flg_if DESC)[ORDINAL(1)]
    END AS update_date,
    CASE ARRAY_AGG(flg_if ORDER BY trn_dt_key DESC, trn_hr DESC, flg_if DESC)[ORDINAL(1)]
        WHEN "1" THEN '{{ task_instance.task_id }}'
        WHEN "0" THEN ARRAY_AGG(update_task_id ORDER BY trn_dt_key DESC, trn_hr DESC, flg_if DESC)[ORDINAL(1)]
    END AS update_task_id,
    CASE ARRAY_AGG(flg_if ORDER BY trn_dt_key DESC, trn_hr DESC, flg_if DESC)[ORDINAL(1)]
        WHEN "1" THEN '{{ task_instance.run_id }}'
        WHEN "0" THEN ARRAY_AGG(update_task_run_id ORDER BY trn_dt_key DESC, trn_hr DESC, flg_if DESC)[ORDINAL(1)]
    END AS update_task_run_id
FROM `{{ params.project_name }}.{{ params.output_dataset }}.{{ params.output_table }}_temp_{{ task_instance.dag_run.conf.loaded_batch }}`
GROUP BY {{ params.pk_expr }}
    