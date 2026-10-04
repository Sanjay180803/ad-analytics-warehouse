-- The mart must not lose or double-count delivery when it joins to dimensions.
-- Totals in the mart have to equal totals in the facts.
with f as (
    select count(*) as fact_impressions, count_if(is_click) as fact_clicks, sum(cost) as fact_spend
    from {{ ref('fct_impressions') }}
),
m as (
    select sum(impressions) as mart_impressions, sum(clicks) as mart_clicks, sum(spend) as mart_spend
    from {{ ref('mart_campaign_daily_performance') }}
)
select *
from f cross join m
where fact_impressions <> mart_impressions
   or fact_clicks      <> mart_clicks
   or abs(fact_spend - mart_spend) > 0.0001
