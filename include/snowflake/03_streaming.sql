-- =============================================================================
-- 03 – Streaming tables (Kafka -> Spark Structured Streaming -> RAW)
--   snow sql -f include/snowflake/03_streaming.sql
--
-- Written by the streaming loader (include/streaming/stream_impressions.py) as
-- LOADER via MERGE, so replayed micro-batches are no-ops. Read by TRANSFORMER (dbt).
-- Column lists must match include/streaming/sinks.py TABLES (checked by tests).
-- =============================================================================

use role loader;
use warehouse loading_wh;
use schema raw.criteo;

-- One row per valid impression event. Same columns and impression_id as
-- IMPRESSIONS (the batch path), plus contract and Kafka lineage columns.
create table if not exists impressions_stream (
  impression_id             varchar(64)    not null,
  event_ts                  timestamp_ntz  not null,
  user_id                   varchar        not null,
  campaign_id               number(38,0)   not null,
  is_click                  boolean        not null,
  is_conversion_path        boolean        not null,
  conversion_ts             timestamp_ntz,
  conversion_id             number(38,0),
  is_attributed             boolean,
  click_pos                 number(9,0),
  click_nb                  number(9,0),
  cost                      number(18,8)   not null,
  cpo                       number(18,6),
  time_since_last_click_s   number(38,0),
  cat1 varchar, cat2 varchar, cat3 varchar, cat4 varchar, cat5 varchar,
  cat6 varchar, cat7 varchar, cat8 varchar, cat9 varchar,
  dt                        date           not null,
  schema_version            varchar        not null,
  produced_at               timestamp_ntz  not null,
  _kafka_topic              varchar        not null,
  _kafka_partition          number(9,0)    not null,
  _kafka_offset             number(38,0)   not null,
  _kafka_ts                 timestamp_ntz,
  _ingested_at              timestamp_ntz  not null,
  constraint pk_impressions_stream primary key (impression_id)   -- informational; MERGE enforces it
)
cluster by (dt)
comment = 'Impression events from Kafka topic ad.impressions.v1, contract-validated by Spark. Insert-if-absent on impression_id.';

-- Every event that failed the data contract, with the reason. Keyed on its Kafka
-- coordinates so a replayed batch never double-counts rejects.
create table if not exists impressions_dlq (
  kafka_topic       varchar        not null,
  kafka_partition   number(9,0)    not null,
  kafka_offset      number(38,0)   not null,
  kafka_ts          timestamp_ntz,
  kafka_key         varchar,
  raw_value         varchar,
  reject_reason     varchar        not null,
  schema_version    varchar,
  _ingested_at      timestamp_ntz  not null,
  constraint pk_impressions_dlq primary key (kafka_topic, kafka_partition, kafka_offset)
)
comment = 'Dead-letter table: contract violations (reason codes: invalid_json, type_mismatch, unsupported_schema_version, missing_required:<f>, constraint:<f>:<rule>, quality:<rule>).';

-- Real-time view: 1-minute tumbling windows per campaign, finalized by a
-- 10-minute event-time watermark. Speed layer only; dbt marts stay the source of truth.
create table if not exists campaign_minute_metrics (
  campaign_id                  number(38,0)  not null,
  window_start                 timestamp_ntz not null,
  window_end                   timestamp_ntz not null,
  impressions                  number(38,0)  not null,
  clicks                       number(38,0)  not null,
  conversion_path_impressions  number(38,0)  not null,
  spend                        number(28,8)  not null,
  approx_users                 number(38,0),
  _updated_at                  timestamp_ntz not null,
  constraint pk_campaign_minute_metrics primary key (campaign_id, window_start)
)
cluster by (window_start)
comment = 'Streaming aggregates per campaign-minute (append mode: each window written once, when final).';

-- Observability: one row per micro-batch of the raw query, and one per batch of every query (listener).
create table if not exists stream_batch_metrics (
  query_name               varchar        not null,
  batch_id                 number(38,0)   not null,
  rows_in                  number(38,0),
  rows_valid               number(38,0),
  rows_dlq                 number(38,0),
  rows_duplicate_in_batch  number(38,0),
  min_event_ts             timestamp_ntz,
  max_event_ts             timestamp_ntz,
  latency_p50_s            float,
  latency_p95_s            float,
  dlq_rate                 float,
  _recorded_at             timestamp_ntz  not null,
  constraint pk_stream_batch_metrics primary key (query_name, batch_id)
);

create table if not exists stream_query_progress (
  query_id                   varchar        not null,
  query_name                 varchar,
  batch_id                   number(38,0)   not null,
  num_input_rows             number(38,0),
  input_rows_per_s           float,
  processed_rows_per_s       float,
  batch_duration_ms          number(38,0),
  rows_dropped_by_watermark  number(38,0),
  state_rows                 number(38,0),
  watermark                  varchar,
  _recorded_at               timestamp_ntz  not null,
  constraint pk_stream_query_progress primary key (query_id, batch_id)
);

-- SLO checks an alert (or the dbt freshness test) can run:
--   freshness: select datediff('second', max(_ingested_at), current_timestamp()) from impressions_stream;
--   DLQ rate, last hour:
--     select sum(rows_dlq) / nullif(sum(rows_in), 0) from stream_batch_metrics
--     where _recorded_at > dateadd('hour', -1, current_timestamp());
--   pipeline latency p95: select max(latency_p95_s) from stream_batch_metrics
--     where _recorded_at > dateadd('hour', -1, current_timestamp());

use role accountadmin;
grant select on all tables in schema raw.criteo to role transformer;   -- future grants from 01_* cover new tables
