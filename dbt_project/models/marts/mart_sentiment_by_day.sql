with stg as (
    select * from {{ ref('stg_tweets') }}
)

select
    date_trunc('day', created_at) as tweet_date,
    sentiment,
    count(*) as tweet_count
from stg
group by tweet_date, sentiment
order by tweet_date, sentiment
