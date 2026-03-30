-- Singular test: no order item should have a negative price or freight value.
-- Returns rows that violate this rule; test passes when 0 rows are returned.
select
    order_id,
    item_sequence,
    price,
    freight_value
from {{ ref('stg_order_items') }}
where price < 0
   or freight_value < 0
