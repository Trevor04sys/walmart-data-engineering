SELECT
    employee_id,

    md5(
        concat_ws(
            '||',
            CAST(employee_id AS STRING),
            CAST(updated_timestamp AS STRING)
        )
    ) AS employee_sk,

    first_name AS employee_first_name,
    last_name AS employee_last_name,
    email AS employee_email,
    job_title,
    salary,
    store_id,
    created_timestamp AS employee_created_timestamp,
    updated_timestamp AS employee_updated_timestamp,
    snapshot_updated_at AS employee_snapshot_updated_at,
    is_active AS employee_is_active,
    processed_at AS employee_processed_at,
    CURRENT_TIMESTAMP() AS employee_gold_processed_at
FROM {{ ref('employees_t') }}