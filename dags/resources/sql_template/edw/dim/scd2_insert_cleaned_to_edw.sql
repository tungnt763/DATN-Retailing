DECLARE max_timestamp TIMESTAMP;
DECLARE effective_start_date_default TIMESTAMP;
SET max_timestamp = TIMESTAMP('{{ task_instance.xcom_pull(task_ids="edw_layer.get_max_timestamp", key="max_timestamp") }}');

IF max_timestamp = TIMESTAMP('1900-01-01 00:00:00') THEN
    TRUNCATE TABLE `{{ params.project_name }}.{{ params.output_dataset }}.{{ params.output_table }}`;
    SET effective_start_date_default = TIMESTAMP('1900-01-01 00:00:00');
ELSE
    SET effective_start_date_default = CURRENT_TIMESTAMP();
END IF;

CREATE TEMP TABLE cleaned_data AS
SELECT
    GENERATE_UUID() AS surr_key,
    {{ params.old_columns }},
    effective_start_date_default AS effective_start_date,
    TIMESTAMP('2261-12-31 23:59:59') AS effective_end_date,
    '1' AS flag_active,
    create_date,
    create_task_id,
    create_task_run_id,
    create_date AS update_date,
    create_task_id AS update_task_id,
    create_task_run_id AS update_task_run_id
FROM `{{ params.project_name }}.{{ params.input_dataset }}.{{ params.input_table }}` target
WHERE NOT EXISTS (
    SELECT 1
    FROM `{{ params.project_name }}.{{ params.output_dataset }}.{{ params.output_table }}` source
    WHERE {{ params.natural_key_expr }}
    AND flag_active = '1'
    AND {{ params.columns_except_natural_key_equal_expr }}
);

MERGE `{{ params.project_name }}.{{ params.output_dataset }}.{{ params.output_table }}` AS target
USING cleaned_data AS source
ON  
    {{ params.natural_key_expr }}
WHEN MATCHED THEN
    UPDATE SET
        effective_end_date = source.effective_start_date - INTERVAL 1 SECOND,
        flag_active = '0',
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