{#
  Criteo has no device field. cat1 is the lowest-cardinality contextual feature,
  so it stands in as a device/placement segment. The dimension is still built the
  way a real dim_device would be: one row per distinct natural key, a stable
  surrogate key, and an unknown member.
#}

with segments as (
    select distinct device_segment_code
    from {{ ref('stg_criteo__impressions') }}
    where device_segment_code <> 'unknown'
)

select
    {{ dbt_utils.generate_surrogate_key(['device_segment_code']) }} as device_key,
    device_segment_code,
    'segment_' || device_segment_code                               as device_segment_label,
    false                                                           as is_unknown
from segments

union all

select '-1', 'unknown', 'Unknown segment', true
