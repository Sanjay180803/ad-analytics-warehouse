{{ config(event_time='event_ts') }}

select
    impression_id,
    event_ts,
    cast(event_ts as date)                         as event_date,
    user_id,
    campaign_id,
    is_click,
    is_conversion_path,
    conversion_ts,
    conversion_id,
    coalesce(is_attributed, false)                 as is_attributed,
    click_pos,
    click_nb,
    cost,
    cpo,
    time_since_last_click_s,
    -- Criteo does not disclose what cat1..cat9 mean. cat1 has the lowest
    -- cardinality, so we treat it as a device/placement segment proxy for dim_device.
    coalesce(nullif(cat1, ''), 'unknown')          as device_segment_code,
    cat2, cat3, cat4, cat5, cat6, cat7, cat8, cat9,
    dt,
    _loaded_at
from {{ source('criteo', 'impressions') }}
