DECLARE max_timestamp TIMESTAMP;
DECLARE effective_start_date_default TIMESTAMP;
SET max_timestamp = TIMESTAMP('{{ task_instance.xcom_pull(task_ids="edw_layer.get_max_timestamp", key="max_timestamp") }}');

IF max_timestamp = TIMESTAMP('1900-01-01 00:00:00') THEN
    TRUNCATE TABLE `{{ params.project_name }}.{{ params.output_dataset }}.{{ params.output_table }}`;
    SET effective_start_date_default = TIMESTAMP('1900-01-01 00:00:00');
ELSE
    SET effective_start_date_default = TIMESTAMP_SECONDS({{ task_instance.dag_run.conf.loaded_batch }});
END IF;

CREATE TEMP TABLE cleaned_data AS
WITH exists_new_update AS (
    SELECT 
        {{ params.natural_key }},
        MIN(effective_start_date) AS next_start_date
    FROM `{{ params.project_name }}.{{ params.output_dataset }}.{{ params.output_table }}`
    WHERE 
        effective_start_date > effective_start_date_default
    GROUP BY {{ params.natural_key }}
)
SELECT
    GENERATE_UUID() AS surr_key,
    {{ params.old_columns }},
    effective_start_date_default AS effective_start_date,
    COALESCE(TIMESTAMP_SUB(e.next_start_date, INTERVAL 1 SECOND), TIMESTAMP('2261-12-31 23:59:59')) AS effective_end_date,
    CASE WHEN e.next_start_date IS NULL THEN '1' ELSE '0' END AS flag_active,
    create_date,
    create_task_id,
    create_task_run_id,
    create_date AS update_date,
    create_task_id AS update_task_id,
    create_task_run_id AS update_task_run_id
FROM `{{ params.project_name }}.{{ params.input_dataset }}.{{ params.input_table }}` target
LEFT JOIN exists_new_update e
    ON {{ params.natural_key_expr_cte }}
WHERE NOT EXISTS (
    SELECT 1
    FROM `{{ params.project_name }}.{{ params.output_dataset }}.{{ params.output_table }}` source
    WHERE {{ params.natural_key_expr }}
    AND ((e.next_start_date IS NULL AND flag_active = '1') 
    OR (e.next_start_date IS NOT NULL AND flag_active = '0' AND effective_start_date = effective_start_date_default))
    AND {{ params.columns_except_natural_key_equal_expr }}
);

MERGE `{{ params.project_name }}.{{ params.output_dataset }}.{{ params.output_table }}` AS target
USING cleaned_data AS source
ON  
    {{ params.natural_key_expr }}
    AND target.flag_active = '1'
    AND source.flag_active = '1'
WHEN MATCHED THEN
    UPDATE SET
        effective_end_date = source.effective_start_date - INTERVAL 1 SECOND,
        flag_active = '0',
        update_date = source.create_date,
        update_task_id = source.create_task_id,
        update_task_run_id = source.create_task_run_id;

MERGE `{{ params.project_name }}.{{ params.output_dataset }}.{{ params.output_table }}` AS target
USING cleaned_data AS source
ON  
    {{ params.natural_key_expr }}
    AND target.flag_active = '0'
    AND source.flag_active = '0'
    AND target.effective_end_date = source.effective_end_date
    AND target.effective_start_date < source.effective_start_date
WHEN MATCHED THEN
    UPDATE SET
        effective_end_date = source.effective_start_date - INTERVAL 1 SECOND,
        update_date = source.create_date,
        update_task_id = source.create_task_id,
        update_task_run_id = source.create_task_run_id;

INSERT INTO `{{ params.project_name }}.{{ params.output_dataset }}.{{ params.output_table }}`
SELECT
    surr_key,
    {{ params.old_columns_except_method }},
    effective_start_date,
    effective_end_date,
    flag_active,
    create_date,
    create_task_id,
    create_task_run_id,
    update_date,
    update_task_id,
    update_task_run_id
FROM cleaned_data;