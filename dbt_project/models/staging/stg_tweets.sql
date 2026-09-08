with source as (
    select * from {{ source('raw', 'tweets_clean') }}
)

select
    "id" as tweet_id,
    "user" as username,
    "created_at" as created_at,
    "sentiment" as sentiment,
    "clean_text" as clean_text
from source
