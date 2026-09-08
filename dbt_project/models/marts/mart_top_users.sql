with stg as (
    select * from {{ ref('stg_tweets') }}
)

select
    username,
    count(*) as total_tweets,
    sum(case when sentiment = 'positive' then 1 else 0 end) as positive_tweets,
    sum(case when sentiment = 'negative' then 1 else 0 end) as negative_tweets,
    round(
        sum(case when sentiment = 'positive' then 1 else 0 end)::float
        / nullif(count(*), 0) * 100,
        2
    ) as positive_pct
from stg
group by username
order by total_tweets desc
limit 100
