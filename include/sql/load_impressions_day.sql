-- Idempotent reload of one day: delete the partition, then COPY it back from S3.
-- Parquet timestamps are UTC; make sure the cast to TIMESTAMP_NTZ does not shift them.
alter session set timezone = 'UTC';
-- Rendered by Airflow ({{ ds }} = the run's logical date). Safe to rerun and backfill.
-- FORCE = TRUE because COPY's 64-day load history would otherwise skip files it
-- has seen before, which is exactly what a rerun needs to load again.
begin;

delete from raw.criteo.impressions where dt = '{{ ds }}'::date;

copy into raw.criteo.impressions (
  impression_id, event_ts, user_id, campaign_id, is_click, is_conversion_path,
  conversion_ts, conversion_id, is_attributed, click_pos, click_nb, cost, cpo,
  time_since_last_click_s, cat1, cat2, cat3, cat4, cat5, cat6, cat7, cat8, cat9,
  raw_source_file, dt, _stage_file
)
from (
  select
    $1:impression_id::varchar,
    $1:event_ts::timestamp_ntz,
    $1:user_id::varchar,
    $1:campaign_id::number,
    $1:is_click::boolean,
    $1:is_conversion_path::boolean,
    $1:conversion_ts::timestamp_ntz,
    $1:conversion_id::number,
    $1:is_attributed::boolean,
    $1:click_pos::number,
    $1:click_nb::number,
    $1:cost::number(18,8),
    $1:cpo::number(18,6),
    $1:time_since_last_click_s::number,
    $1:cat1::varchar, $1:cat2::varchar, $1:cat3::varchar, $1:cat4::varchar, $1:cat5::varchar,
    $1:cat6::varchar, $1:cat7::varchar, $1:cat8::varchar, $1:cat9::varchar,
    $1:source_file::varchar,
    '{{ ds }}'::date,
    metadata$filename
  from @raw.criteo.clean_stage/impressions/dt={{ ds }}/
)
pattern = '.*[.]parquet'
file_format = (format_name = raw.criteo.ff_parquet)
force = true
on_error = abort_statement;

commit;
