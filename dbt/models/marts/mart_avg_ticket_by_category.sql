/*
  Metric: Average ticket and revenue share by product category
  Grain:  one row per category (all time, delivered orders only)

  Helps identify which product categories drive the most revenue
  and which have the highest avg ticket vs freight ratio.
*/
with order_items as (
    select
        order_id,
        product_id,
        price,
        freight_value,
        total_item_value
    from {{ ref('stg_order_items') }}
),

products as (
    select product_id, category
    from {{ ref('stg_products') }}
),

delivered_orders as (
    select order_id
    from {{ ref('stg_orders') }}
    where order_status = 'delivered'
),

base as (
    select
        coalesce(p.category, 'uncategorized')  as category,
        i.order_id,
        sum(i.price)             as items_revenue,
        sum(i.freight_value)     as freight_revenue,
        sum(i.total_item_value)  as gross_revenue
    from order_items i
    inner join delivered_orders o on i.order_id    = o.order_id
    left join  products         p on i.product_id  = p.product_id
    group by 1, 2
)

select
    category,
    count(distinct order_id)                                              as total_orders,
    round(sum(items_revenue),  2)                                         as total_revenue,
    round(avg(items_revenue),  2)                                         as avg_ticket,
    round(avg(gross_revenue),  2)                                         as avg_ticket_with_freight,
    round(
        sum(freight_revenue) / nullif(sum(items_revenue), 0) * 100, 1
    )                                                                     as freight_pct
from base
group by 1
order by total_revenue desc
