{#
  Grain: one row per click. In Criteo a click is a flag on its impression (at most
  one click per impression, no separate click timestamp), so click_id reuses the
  impression_id and click_ts is the impression time.
#}
{{
  config(
    materialized='incremental',
    incremental_strategy='microbatch',
    event_time='click_ts',
    batch_size='day',
    begin=var('anchor_date'),
    lookback=1,
  )
}}

select
    impression_id                as click_id,
    impression_id,
    event_ts                     as click_ts,
    date_key,
    campaign_sk,
    campaign_id,
    device_key,
    user_id,
    click_pos,
    click_nb,
    cost,
    conversion_id
from {{ ref('fct_impressions') }}
where is_click
