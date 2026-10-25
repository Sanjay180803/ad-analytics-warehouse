-- Streamed impressions, shaped exactly like stg_criteo__impressions
-- (same columns, same names, same order) plus Kafka lineage.
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
    coalesce(nullif(cat1, ''), 'unknown')          as device_segment_code,
    cat2, cat3, cat4, cat5, cat6, cat7, cat8, cat9,
    dt,
    _ingested_at                                   as _loaded_at,
    -- streaming lineage
    schema_version,
    _kafka_topic,
    _kafka_partition,
    _kafka_offset,
    datediff('second', _kafka_ts, _ingested_at)    as ingest_lag_s
from {{ source('criteo', 'impressions_stream') }}
