SELECT
    customer_id,
    COUNT(*) AS current_versions
FROM {{ ref('dim_customers') }}
WHERE dbt_valid_to = TO_DATE('9999-12-31')
GROUP BY customer_id
HAVING COUNT(*) <> 1