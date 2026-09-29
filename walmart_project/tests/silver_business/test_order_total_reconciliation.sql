SELECT
    order_id,
    MAX(total_amount) AS order_total,
    SUM(line_amount) AS item_total
FROM {{ ref('obt_b') }}
GROUP BY order_id
HAVING ROUND(MAX(total_amount), 2) <> ROUND(SUM(line_amount), 2)