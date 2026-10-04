{#
  Grain: one row per campaign per day.

  Delivery metrics (impressions, clicks, spend) are dated by impression day.
  Conversions are dated by conversion day and credited to the campaign chosen in
  fct_conversions (last click). That is the usual reporting convention, and it
  means CVR on a single day can exceed what that day's clicks alone produced;
  use longer windows for stable rates.

  Campaign attributes are the version in effect at the end of the day.
#}

with delivery as (
    select
        campaign_id,
        date_key,
        count(*)                 as impressions,
        count_if(is_click)       as clicks,
        sum(cost)                as spend,
        count(distinct user_id)  as reached_users
    from {{ ref('fct_impressions') }}
    group by 1, 2
),

conversions as (
    select
        credited_campaign_id     as campaign_id,
        conversion_date_key      as date_key,
        count(*)                 as conversions,
        count_if(is_attributed_to_criteo) as attributed_conversions
    from {{ ref('fct_conversions') }}
    group by 1, 2
),

combined as (
    select
        coalesce(d.campaign_id, c.campaign_id)   as campaign_id,
        coalesce(d.date_key, c.date_key)         as date_key,
        coalesce(d.impressions, 0)               as impressions,
        coalesce(d.clicks, 0)                    as clicks,
        coalesce(d.spend, 0)                     as spend,
        coalesce(d.reached_users, 0)             as reached_users,
        coalesce(c.conversions, 0)               as conversions,
        coalesce(c.attributed_conversions, 0)    as attributed_conversions
    from delivery d
    full outer join conversions c
        on d.campaign_id = c.campaign_id and d.date_key = c.date_key
)

select
    {{ dbt_utils.generate_surrogate_key(['x.campaign_id', 'x.date_key']) }} as campaign_day_id,
    x.date_key,
    dd.date_day,
    x.campaign_id,
    dc.campaign_sk,
    dc.advertiser_id,
    dc.campaign_vertical,
    dc.budget_tier,
    dc.bid_strategy,
    x.impressions,
    x.clicks,
    x.conversions,
    x.attributed_conversions,
    x.reached_users,
    x.spend,
    div0(x.clicks, x.impressions)                 as ctr,
    div0(x.conversions, x.clicks)                 as cvr,
    div0(x.spend, x.clicks)                       as cpc,
    div0(x.spend, x.impressions) * 1000           as cpm,
    iff(x.attributed_conversions = 0, null, x.spend / x.attributed_conversions) as cpa
from combined x
join {{ ref('dim_date') }} dd
    on dd.date_key = x.date_key
left join {{ ref('dim_campaign') }} dc
    on  dc.campaign_id = x.campaign_id
    and dc.valid_from <  dateadd('day', 1, dd.date_day)::timestamp_ntz
    and dc.valid_to   >= dateadd('day', 1, dd.date_day)::timestamp_ntz
