-- Lambda-style reconciliation: for every closed day that BOTH paths loaded,
-- the stream and the batch must contain exactly the same impressions.
-- Possible because both compute impression_id the same way (SHA-256 of the raw row).
-- Warn, not error: during a stream outage, batch is the source of truth and
-- this test is how we find out what the stream missed.
{{ config(severity='warn') }}

with batch as (
    select dt, impression_id from {{ ref('stg_criteo__impressions') }}
),
stream as (
    select dt, impression_id from {{ ref('stg_criteo__impressions_stream') }}
),
shared_days as (
    select dt from (select distinct dt from batch)
    intersect
    select dt from (select distinct dt from stream)
),
diff as (
    select coalesce(b.dt, s.dt) as dt,
           count_if(s.impression_id is null) as missing_in_stream,
           count_if(b.impression_id is null) as missing_in_batch
    from (select * from batch where dt in (select dt from shared_days)) b
    full outer join (select * from stream where dt in (select dt from shared_days)) s
      on b.impression_id = s.impression_id
    group by 1
)
select * from diff
where dt < current_date
  and (missing_in_stream > 0 or missing_in_batch > 0)
