SELECT
    f.order_item_id,
    f.product_id,
    f.product_sk,
    p.product_sk AS expected_product_sk,
    o.order_timestamp
FROM {{ ref('fact_orders') }} AS f

JOIN {{ ref('obt_b') }} AS o
    ON f.order_item_id = o.order_item_id

LEFT JOIN {{ ref('dim_products') }} AS p
    ON o.product_id = p.product_id
    AND o.order_timestamp >= p.dbt_valid_from
    AND o.order_timestamp < p.dbt_valid_to

WHERE f.product_sk <> COALESCE(p.product_sk, 'UNKNOWN')