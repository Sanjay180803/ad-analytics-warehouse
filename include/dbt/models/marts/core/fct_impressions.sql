{#
  Grain: one row per impression.

  Microbatch incremental: dbt splits the work into daily batches on event_ts and
  replaces each batch atomically. Airflow passes --event-time-start/--event-time-end
  for the run's day, so a rerun or backfill of any day rebuilds exactly that day.
  Upstream refs that declare event_time (stg_criteo__impressions) are filtered to
  the batch automatically; dimensions are not, which is what we want.
#}
{{
  config(
    materialized='incremental',
    incremental_strategy='microbatch',
    event_time='event_ts',
    batch_size='day',
    begin=var('anchor_date'),
    lookback=1,
    cluster_by=['date_key'],
  )
}}

with impressions as (
    select * from {{ ref('stg_criteo__impressions') }}
),

campaigns as (
    select campaign_sk, campaign_id, valid_from, valid_to
    from {{ ref('dim_campaign') }}
    where campaign_id <> -1
),

devices as (
    select device_key, device_segment_code from {{ ref('dim_device') }}
)

select
    i.impression_id,
    i.event_ts,
    to_number(to_char(i.event_date, 'YYYYMMDD'))   as date_key,
    coalesce(c.campaign_sk, '-1')                   as campaign_sk,
    i.campaign_id,
    coalesce(d.device_key, '-1')                    as device_key,
    i.user_id,
    i.cost,
    i.is_click,
    i.is_conversion_path,
    i.conversion_id,
    i.conversion_ts,
    i.is_attributed,
    i.click_pos,
    i.click_nb,
    i.cpo,
    i.time_since_last_click_s
from impressions i
-- as-of join: the campaign version in effect when the impression was served
left join campaigns c
    on  i.campaign_id = c.campaign_id
    and i.event_ts >= c.valid_from
    and i.event_ts <  c.valid_to
left join devices d
    on i.device_segment_code = d.device_segment_code
