SELECT
    order_id,

    customer_id,
    store_id,
    payment_method,
    order_status,
    order_timestamp,
    total_amount,

    order_created_timestamp,
    order_updated_timestamp,
    order_is_active,
    order_processed_at,
    obt_b_processed_at,

    CURRENT_TIMESTAMP() AS order_gold_processed_at

FROM {{ ref('obt_b') }}

QUALIFY ROW_NUMBER() OVER (
    PARTITION BY order_id
    ORDER BY order_updated_timestamp DESC
) = 1