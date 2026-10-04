{#
  Grain: one row per conversion (conversion_id).

  In Criteo every impression on a converting user's path carries the same
  conversion_id and conversion_ts, and `attribution` says whether the advertiser
  credited the conversion to Criteo. We credit the conversion with last-click
  attribution (last clicked impression before the conversion), falling back to
  last touch when the path has no click.

  Materialized as a full table, not microbatch: a conversion's path spans several
  impression days, so a single-day batch would see an incomplete path. At ~45K
  conversions a full rebuild costs seconds and is always correct, even when days
  are backfilled out of order.
#}

with path as (
    select *
    from {{ ref('fct_impressions') }}
    where conversion_id is not null
      and event_ts <= conversion_ts
),

ranked as (
    select
        *,
        row_number() over (partition by conversion_id order by event_ts desc, impression_id) as touch_rank,
        row_number() over (partition by conversion_id
                           order by iff(is_click, 0, 1), event_ts desc, impression_id)       as click_rank
    from path
),

agg as (
    select
        conversion_id,
        max(conversion_ts)                 as conversion_ts,
        any_value(user_id)                 as user_id,
        count(*)                           as path_impressions,
        count_if(is_click)                 as path_clicks,
        boolor_agg(is_attributed)          as is_attributed_to_criteo,
        min(event_ts)                      as first_touch_ts,
        sum(cost)                          as path_cost
    from path
    group by conversion_id
)

select
    a.conversion_id,
    a.conversion_ts,
    to_number(to_char(a.conversion_ts::date, 'YYYYMMDD'))  as conversion_date_key,
    a.user_id,
    credit.impression_id                                   as credited_impression_id,
    credit.campaign_sk                                     as credited_campaign_sk,
    credit.campaign_id                                     as credited_campaign_id,
    credit.device_key                                      as credited_device_key,
    credit.is_click                                        as credited_by_click,
    last_touch.impression_id                               as last_touch_impression_id,
    a.is_attributed_to_criteo,
    a.path_impressions,
    a.path_clicks,
    a.path_cost,
    credit.cpo                                             as cpo,
    datediff('second', a.first_touch_ts, a.conversion_ts)  as seconds_first_touch_to_conversion
from agg a
join ranked credit
    on credit.conversion_id = a.conversion_id and credit.click_rank = 1
join ranked last_touch
    on last_touch.conversion_id = a.conversion_id and last_touch.touch_rank = 1
