CREATE TABLE IF NOT EXISTS `{{ params.project_name }}.{{ params.output_dataset }}.{{ params.table_name }}_invalid` AS
SELECT
    *
FROM `{{ params.project_name }}.{{ params.output_dataset }}.{{ params.table_name }}_temp_{{ task_instance.dag_run.conf.loaded_batch }}`
WHERE FALSE;

INSERT INTO `{{ params.project_name }}.{{ params.output_dataset }}.{{ params.table_name }}_invalid`
SELECT
    *
FROM `{{ params.project_name }}.{{ params.output_dataset }}.{{ params.table_name }}_temp_{{ task_instance.dag_run.conf.loaded_batch }}`
WHERE 
    is_valid_cstmr_id = false
    OR is_valid_prd_id = false
    OR is_valid_dscnt_id = false
    OR is_valid_line_ttl = false
    OR is_valid_str_id = false
    OR is_valid_emply_id = false;