/*
  Metric: Monthly order volume and revenue
  Grain:  (order_month, order_status)

  Shows the evolution of order volume, revenue and status distribution over time.
  Useful for seasonality analysis and funnel health monitoring.
*/
with orders as (
    select
        order_id,
        customer_id,
        order_status,
        purchased_at
    from {{ ref('stg_orders') }}
    where purchased_at is not null
),

payments as (
    select
        order_id,
        sum(payment_value) as order_revenue
    from {{ ref('stg_order_payments') }}
    group by 1
)

select
    date_trunc('month', o.purchased_at)::date  as order_month,
    o.order_status                              as status,
    count(distinct o.order_id)                  as total_orders,
    count(distinct o.customer_id)               as unique_customers,
    round(sum(p.order_revenue), 2)              as total_revenue,
    round(avg(p.order_revenue), 2)              as avg_ticket
from orders o
left join payments p on o.order_id = p.order_id
group by 1, 2
order by 1, 2
