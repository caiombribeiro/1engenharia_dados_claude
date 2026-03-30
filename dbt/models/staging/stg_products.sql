with source as (
    select * from {{ source('raw', 'products') }}
),

translation as (
    select * from {{ source('raw', 'product_category_name_translation') }}
),

cleaned as (
    select
        p.product_id,
        coalesce(
            t.product_category_name_english,
            p.product_category_name,
            'uncategorized'
        )                                    as category,
        p.product_name_lenght::int           as name_length,
        p.product_description_lenght::int    as description_length,
        p.product_photos_qty::int            as photos_qty,
        p.product_weight_g::numeric          as weight_g,
        p.product_length_cm::numeric         as length_cm,
        p.product_height_cm::numeric         as height_cm,
        p.product_width_cm::numeric          as width_cm
    from source p
    left join translation t using (product_category_name)
    where p.product_id is not null
)

select * from cleaned
