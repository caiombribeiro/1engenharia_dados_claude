with source as (
    select * from {{ source('raw', 'sellers') }}
),

cleaned as (
    select
        seller_id,
        seller_zip_code_prefix::int  as zip_code_prefix,
        initcap(seller_city)         as city,
        upper(seller_state)          as state
    from source
    where seller_id is not null
)

select * from cleaned
