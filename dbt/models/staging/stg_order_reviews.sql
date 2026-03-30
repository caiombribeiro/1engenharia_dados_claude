with source as (
    select * from {{ source('raw', 'order_reviews') }}
),

cleaned as (
    select
        review_id,
        order_id,
        review_score::int                          as score,
        nullif(review_comment_title,   '')         as comment_title,
        nullif(review_comment_message, '')         as comment_message,
        review_creation_date::timestamp            as created_at,
        review_answer_timestamp::timestamp         as answered_at
    from source
    where review_id is not null
)

select * from cleaned
