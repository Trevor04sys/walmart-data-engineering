{{
    config(
        materialized='incremental',
        unique_key='employee_id'
    )
}}

SELECT
    *,
    
    CASE
        WHEN {{ is_incremental() }}
            THEN updated_timestamp
        ELSE created_timestamp
    END AS snapshot_updated_at,

    current_timestamp() AS processed_at

FROM {{ source('walmart_databricks', 'employees') }}

{% if is_incremental() %}

WHERE updated_timestamp >
    (
        SELECT COALESCE(
            MAX(updated_timestamp),
            '1900-01-01'
        )
        FROM {{ this }}
    )

{% endif %}