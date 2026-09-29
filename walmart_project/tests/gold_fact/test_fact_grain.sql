select 
    order_item_id,
    count(*) as row_count
from {{ ref('fact_orders') }}
group by order_item_id
having count(*) > 1