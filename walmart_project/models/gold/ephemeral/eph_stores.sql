SELECT
    store_id,

    md5(
        concat_ws(
            '||',
            CAST(store_id AS STRING),
            CAST(updated_timestamp AS STRING)
        )
    ) AS store_sk,

    store_name,
    city AS store_city,
    province AS store_province,
    country AS store_country,
    created_timestamp AS store_created_timestamp,
    updated_timestamp AS store_updated_timestamp,
    snapshot_updated_at AS store_snapshot_updated_at,
    is_active AS store_is_active,
    processed_at AS store_processed_at,
    CURRENT_TIMESTAMP() AS store_gold_processed_at
FROM {{ ref('stores_t') }}