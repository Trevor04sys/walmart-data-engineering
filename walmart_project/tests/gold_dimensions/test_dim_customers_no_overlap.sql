WITH versions AS (

    SELECT
        customer_id,
        dbt_valid_from,
        dbt_valid_to,

        LEAD(dbt_valid_from) OVER (
            PARTITION BY customer_id
            ORDER BY dbt_valid_from
        ) AS next_valid_from

    FROM {{ ref('dim_customers') }}

)

SELECT
    customer_id,
    dbt_valid_from,
    dbt_valid_to,
    next_valid_from

FROM versions

WHERE dbt_valid_to > next_valid_from