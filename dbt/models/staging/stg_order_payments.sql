with source as (
    select * from {{ source('raw', 'order_payments') }}
),

cleaned as (
    select
        order_id,
        payment_sequential::int        as payment_sequence,
        payment_type,
        payment_installments::int      as installments,
        payment_value::numeric(10, 2)  as payment_value
    from source
    where order_id is not null
)

select * from cleaned
