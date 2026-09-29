SELECT 
    f.order_item_id,
    f.order_id,

    -- Dimension surrogate keys
    COALESCE(c.customer_sk, 'UNKNOWN') AS customer_sk,
    COALESCE(p.product_sk, 'UNKNOWN') AS product_sk,
    COALESCE(s.store_sk, 'UNKNOWN') AS store_sk,

    -- Business keys retained for traceability
    f.product_id,
    f.customer_id,
    f.store_id,

    -- Measures
    f.quantity,
    f.unit_price,
    f.line_amount

FROM {{ ref('obt_b') }} AS f

LEFT JOIN {{ ref('dim_customers') }} AS c
    ON f.customer_id = c.customer_id
    AND f.order_timestamp >= c.dbt_valid_from
    AND f.order_timestamp < c.dbt_valid_to

LEFT JOIN {{ ref('dim_products') }} AS p
    ON f.product_id = p.product_id
    AND f.order_timestamp >= p.dbt_valid_from
    AND f.order_timestamp < p.dbt_valid_to

LEFT JOIN {{ ref('dim_stores') }} AS s
    ON f.store_id = s.store_id
    AND f.order_timestamp >= s.dbt_valid_from
    AND f.order_timestamp < s.dbt_valid_to