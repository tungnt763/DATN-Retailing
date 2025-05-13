TRUNCATE TABLE `{{ params.project_name }}.{{ params.output_dataset }}.{{ params.output_table }}`;

INSERT INTO `{{ params.project_name }}.{{ params.output_dataset }}.{{ params.output_table }}`
SELECT
    {{ params.pk }}
    ,ARRAY_AGG(trn_id ORDER BY acnt_update_dtime DESC, flg_if DESC)[ORDINAL(1)] AS trn_id
    ,ARRAY_AGG(trn_hr ORDER BY acnt_update_dtime DESC, flg_if DESC)[ORDINAL(1)] AS trn_hr
    ,ARRAY_AGG(trn_dt_key ORDER BY acnt_update_dtime DESC, flg_if DESC)[ORDINAL(1)] AS trn_dt_key
    ,ARRAY_AGG(trn_cstmr_key ORDER BY acnt_update_dtime DESC, flg_if DESC)[ORDINAL(1)] AS trn_cstmr_key
    ,ARRAY_AGG(trn_prd_key ORDER BY acnt_update_dtime DESC, flg_if DESC)[ORDINAL(1)] AS trn_prd_key
    ,ARRAY_AGG(trn_str_key ORDER BY acnt_update_dtime DESC, flg_if DESC)[ORDINAL(1)] AS trn_str_key
    ,ARRAY_AGG(trn_emply_key ORDER BY acnt_update_dtime DESC, flg_if DESC)[ORDINAL(1)] AS trn_emply_key
    ,ARRAY_AGG(trn_dscnt_key ORDER BY acnt_update_dtime DESC, flg_if DESC)[ORDINAL(1)] AS trn_dscnt_key
    ,ARRAY_AGG(trn_crncy_key ORDER BY acnt_update_dtime DESC, flg_if DESC)[ORDINAL(1)] AS trn_crncy_key
    ,ARRAY_AGG(trn_sale_qty ORDER BY acnt_update_dtime DESC, flg_if DESC)[ORDINAL(1)] AS trn_sale_qty
    ,ARRAY_AGG(trn_reg_unit_prc ORDER BY acnt_update_dtime DESC, flg_if DESC)[ORDINAL(1)] AS trn_reg_unit_prc
    ,ARRAY_AGG(trn_dscnt_unit_prc ORDER BY acnt_update_dtime DESC, flg_if DESC)[ORDINAL(1)] AS trn_dscnt_unit_prc
    ,ARRAY_AGG(trn_net_unit_prc ORDER BY acnt_update_dtime DESC, flg_if DESC)[ORDINAL(1)] AS trn_net_unit_prc
    ,ARRAY_AGG(trn_ext_sale_dol ORDER BY acnt_update_dtime DESC, flg_if DESC)[ORDINAL(1)] AS trn_ext_sale_dol
    ,ARRAY_AGG(trn_ext_dscnt_dol ORDER BY acnt_update_dtime DESC, flg_if DESC)[ORDINAL(1)] AS trn_ext_dscnt_dol
    ,ARRAY_AGG(trn_ext_cost_dol ORDER BY acnt_update_dtime DESC, flg_if DESC)[ORDINAL(1)] AS trn_ext_cost_dol
    ,ARRAY_AGG(trn_ext_grs_prft_dol ORDER BY acnt_update_dtime DESC, flg_if DESC)[ORDINAL(1)] AS trn_ext_grs_prft_dol
    ,ARRAY_AGG(trn_line_ttl ORDER BY acnt_update_dtime DESC, flg_if DESC)[ORDINAL(1)] AS trn_line_ttl
    ,ARRAY_AGG(trn_sz ORDER BY acnt_update_dtime DESC, flg_if DESC)[ORDINAL(1)] AS trn_sz
    ,ARRAY_AGG(trn_cl ORDER BY acnt_update_dtime DESC, flg_if DESC)[ORDINAL(1)] AS trn_cl
    ,ARRAY_AGG(trn_type ORDER BY acnt_update_dtime DESC, flg_if DESC)[ORDINAL(1)] AS trn_type
    ,ARRAY_AGG(trn_pymnt_mthd ORDER BY acnt_update_dtime DESC, flg_if DESC)[ORDINAL(1)] AS trn_pymnt_mthd
    ,CASE ARRAY_AGG(flg_if ORDER BY flg_if ASC)[ORDINAL(1)]
        WHEN "1" THEN CURRENT_TIMESTAMP()
        WHEN "0" THEN ARRAY_AGG(create_date ORDER BY flg_if ASC)[ORDINAL(1)]
    END AS create_date
    ,CASE ARRAY_AGG(flg_if ORDER BY flg_if ASC)[ORDINAL(1)]
        WHEN "1" THEN '{{ task_instance.task_id }}'
        WHEN "0" THEN ARRAY_AGG(create_task_id ORDER BY flg_if ASC)[ORDINAL(1)]
    END AS create_task_id
    ,CASE ARRAY_AGG(flg_if ORDER BY flg_if ASC)[ORDINAL(1)]
        WHEN "1" THEN '{{ task_instance.run_id }}'
        WHEN "0" THEN ARRAY_AGG(create_task_run_id ORDER BY flg_if ASC)[ORDINAL(1)]
    END AS create_task_run_id
    ,CASE ARRAY_AGG(flg_if ORDER BY acnt_update_dtime DESC, flg_if DESC)[ORDINAL(1)]
        WHEN "1" THEN CURRENT_TIMESTAMP()
        WHEN "0" THEN ARRAY_AGG(update_date ORDER BY acnt_update_dtime DESC, flg_if DESC)[ORDINAL(1)]
    END AS update_date
    ,CASE ARRAY_AGG(flg_if ORDER BY acnt_update_dtime DESC, flg_if DESC)[ORDINAL(1)]
        WHEN "1" THEN '{{qn}}'
        WHEN "0" THEN ARRAY_AGG(update_task_id ORDER BY acnt_update_dtime DESC, flg_if DESC)[ORDINAL(1)]
    END AS update_task_id
    ,CASE ARRAY_AGG(flg_if ORDER BY acnt_update_dtime DESC, flg_if DESC)[ORDINAL(1)]
        WHEN "1" THEN '{{taskRunId}}'
        WHEN "0" THEN ARRAY_AGG(update_task_run_id ORDER BY acnt_update_dtime DESC, flg_if DESC)[ORDINAL(1)]
    END AS update_task_run_id
FROM `{{ params.project_name }}.{{ params.output_dataset }}.{{ params.output_table }}_temp`
GROUP BY {{ params.pk }}
    