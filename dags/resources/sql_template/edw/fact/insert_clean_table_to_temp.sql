INSERT INTO `{{ params.project_name }}.{{ params.output_dataset }}.{{ params.output_table }}_temp_{{ task_instance.dag_run.conf.loaded_batch }}`
SELECT
    {{ params.method_exprs }},
    CURRENT_TIMESTAMP() AS create_date,
    '{{ task_instance.task_id }}' AS create_task_id,
    '{{ task_instance.run_id }}' AS create_task_run_id,
    CURRENT_TIMESTAMP() AS update_date,
    '{{ task_instance.task_id }}' AS update_task_id,
    '{{ task_instance.run_id }}' AS update_task_run_id,
    '1'
FROM `{{ params.project_name }}.{{ params.input_dataset }}.{{ params.input_table }}` AS t
JOIN `datn-retailing.edw.dim_customers` AS ctm
    ON t.trn_cstmr_id = ctm.cstmr_id
    AND ctm.effective_start_date <= t.trn_date
    AND ctm.effective_end_date >= t.trn_date
JOIN `datn-retailing.edw.dim_products` AS p
    ON t.trn_prd_id = p.prd_id
    AND p.effective_start_date <= t.trn_date
    AND p.effective_end_date >= t.trn_date
JOIN `datn-retailing.edw.dim_stores` AS s
    ON t.trn_str_id = s.str_id
    AND s.effective_start_date <= t.trn_date
    AND s.effective_end_date >= t.trn_date
JOIN `datn-retailing.edw.dim_employees` AS e
    ON t.trn_emply_id = e.emply_id
JOIN `datn-retailing.edw.dim_currency` AS crncy
    ON t.trn_crncy = crncy.crncy_cd
    AND crncy.effective_start_date <= t.trn_date
    AND crncy.effective_end_date >= t.trn_date
JOIN `datn-retailing.edw.dim_discounts` AS d
    ON DATE(t.trn_date) >= d.dscnt_start_date
    AND DATE(t.trn_date) <= d.dscnt_end_date
    AND t.trn_dscnt = d.dscnt_value
    AND ((p.prd_ctgry = d.dscnt_ctgry AND p.prd_sub_ctgry = d.dscnt_sub_ctgry) OR d.dscnt_ctgry = 'Unknown')
    AND d.effective_start_date <= t.trn_date
    AND d.effective_end_date >= t.trn_date
JOIN `datn-retailing.edw.dim_weather` AS w
    ON DATE(t.trn_date) = w.wthr_date
    AND s.str_lat = w.wthr_lat
    AND s.str_lon = w.wthr_lon
JOIN `datn-retailing.edw.dim_locations` AS l1
    ON ctm.cstmr_city = l1.lct_raw_city
    AND ctm.cstmr_cntry = l1.lct_raw_cntry
JOIN `datn-retailing.edw.dim_locations` AS l2
    ON s.str_city = l2.lct_raw_city
    AND s.str_cntry = l2.lct_raw_cntry
WHERE
    t.create_date > TIMESTAMP('{{ task_instance.xcom_pull(task_ids="clean_layer.get_max_timestamp", key="max_timestamp") }}')
;