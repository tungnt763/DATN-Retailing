CREATE TABLE IF NOT EXISTS `{{ params.project_name }}.{{ params.output_dataset }}.{{ params.table_name }}_invalid` AS
SELECT
    *
FROM `{{ params.project_name }}.{{ params.output_dataset }}.{{ params.table_name }}_temp`
WHERE FALSE;

INSERT INTO `{{ params.project_name }}.{{ params.output_dataset }}.{{ params.table_name }}_invalid`
SELECT
    *
FROM `{{ params.project_name }}.{{ params.output_dataset }}.{{ params.table_name }}_temp`
WHERE NOT(
    is_valid_cstmr_id
    AND is_valid_prd_id
    AND is_valid_dscnt_id
    AND is_valid_line_ttl
    AND is_valid_str_id
    AND is_valid_emply_id
)