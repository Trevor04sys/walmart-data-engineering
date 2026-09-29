SELECT
    f.order_item_id,
    f.store_id,
    f.store_sk,
    s.store_sk AS expected_store_sk,
    o.order_timestamp
FROM {{ ref('fact_orders') }} AS f

JOIN {{ ref('obt_b') }} AS o
    ON f.order_item_id = o.order_item_id

LEFT JOIN {{ ref('dim_stores') }} AS s
    ON o.store_id = s.store_id
    AND o.order_timestamp >= s.dbt_valid_from
    AND o.order_timestamp < s.dbt_valid_to

WHERE f.store_sk <> COALESCE(s.store_sk, 'UNKNOWN')