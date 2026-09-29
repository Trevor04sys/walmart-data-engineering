SELECT
    product_id,

    md5(
        concat_ws(
            '||',
            CAST(product_id AS STRING),
            CAST(updated_timestamp AS STRING)
        )
    ) AS product_sk,

    product_name,
    category,
    brand,
    price,
    created_timestamp AS product_created_timestamp,
    updated_timestamp AS product_updated_timestamp,
    snapshot_updated_at AS product_snapshot_updated_at,
    is_active AS product_is_active,
    processed_at AS product_processed_at,
    CURRENT_TIMESTAMP() AS product_gold_processed_at
FROM {{ ref('products_t') }}