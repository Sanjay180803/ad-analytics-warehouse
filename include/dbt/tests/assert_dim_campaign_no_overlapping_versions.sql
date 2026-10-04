-- SCD2 integrity: a campaign's validity windows must not overlap or leave gaps,
-- and each campaign has exactly one current version. Returns offending rows.
with v as (
    select
        campaign_id,
        campaign_sk,
        valid_from,
        valid_to,
        is_current,
        lead(valid_from) over (partition by campaign_id order by valid_from) as next_valid_from
    from {{ ref('dim_campaign') }}
    where campaign_id <> -1
)

select campaign_id, campaign_sk, 'gap_or_overlap' as problem
from v
where next_valid_from is not null and next_valid_from <> valid_to

union all

select campaign_id, null, 'current_versions=' || count_if(is_current)
from v
group by campaign_id
having count_if(is_current) <> 1
