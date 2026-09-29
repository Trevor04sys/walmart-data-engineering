SELECT
    f.order_item_id,
    f.customer_id,
    f.customer_sk,
    c.customer_sk AS expected_customer_sk,
    o.order_timestamp
FROM {{ ref('fact_orders') }} AS f

JOIN {{ ref('obt_b') }} AS o
    ON f.order_item_id = o.order_item_id

LEFT JOIN {{ ref('dim_customers') }} AS c
    ON o.customer_id = c.customer_id
    AND o.order_timestamp >= c.dbt_valid_from
    AND o.order_timestamp < c.dbt_valid_to

WHERE f.customer_sk <> COALESCE(c.customer_sk, 'UNKNOWN')