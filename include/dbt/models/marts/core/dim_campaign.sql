{#
  SCD Type 2 built from a change log with window functions.

  Why not a dbt snapshot? Snapshots record changes they observe at run time, so
  they cannot reconstruct history that happened before the first snapshot, and
  a backfill can't replay them. The source here is already an event log of
  changes, so deriving validity windows from it is deterministic, replayable and
  fully rebuildable. If the source were a current-state table (no history), a
  snapshot would be the right tool.

  Windows are half-open: valid_from <= ts < valid_to.
#}

with changes as (
    select * from {{ ref('stg_criteo__campaign_changes') }}
),

windows as (
    select
        *,
        lead(changed_at) over (partition by campaign_id order by changed_at) as next_changed_at,
        row_number()     over (partition by campaign_id order by changed_at) as version_number
    from changes
),

versions as (
    select
        {{ dbt_utils.generate_surrogate_key(['campaign_id', 'changed_at']) }} as campaign_sk,
        campaign_id,
        advertiser_id,
        campaign_vertical,
        budget_tier,
        bid_strategy,
        daily_budget_usd,
        change_reason,
        -- the first version is back-dated so no impression falls before a campaign's history
        iff(version_number = 1, '1900-01-01'::timestamp_ntz, changed_at)   as valid_from,
        coalesce(next_changed_at, '9999-12-31'::timestamp_ntz)              as valid_to,
        next_changed_at is null                                             as is_current,
        version_number
    from windows
)

select * from versions

union all

-- unknown member: facts never point at NULL
select
    '{{ var("unknown_key", "-1") }}'      as campaign_sk,
    -1                                     as campaign_id,
    null, 'unknown', 'unknown', 'unknown', null, null,
    '1900-01-01'::timestamp_ntz, '9999-12-31'::timestamp_ntz, true, 1
