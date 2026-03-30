with source as (
    select * from {{ source('raw', 'customers') }}
),

cleaned as (
    select
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix::int  as zip_code_prefix,
        initcap(customer_city)         as city,
        upper(customer_state)          as state
    from source
    where customer_id is not null
)

select * from cleaned
