/*
  Metric: Revenue by state (delivered orders only)
  Grain:  (state, year)

  Joins customers → orders → payments to calculate total and average
  revenue per Brazilian state per year.
*/
with orders as (
    select order_id, customer_id, purchased_at
    from {{ ref('stg_orders') }}
    where order_status = 'delivered'
),

customers as (
    select customer_id, state
    from {{ ref('stg_customers') }}
),

payments as (
    select
        order_id,
        sum(payment_value) as order_revenue
    from {{ ref('stg_order_payments') }}
    group by 1
),

base as (
    select
        c.state,
        date_trunc('year', o.purchased_at)::date  as year,
        o.order_id,
        p.order_revenue
    from orders o
    join customers c on o.customer_id = c.customer_id
    join payments  p on o.order_id    = p.order_id
)

select
    state,
    year,
    count(distinct order_id)          as total_orders,
    round(sum(order_revenue), 2)      as total_revenue,
    round(avg(order_revenue), 2)      as avg_ticket
from base
group by 1, 2
order by total_revenue desc
