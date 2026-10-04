select
    campaign_id,
    advertiser_id,
    lower(campaign_vertical)                                  as campaign_vertical,
    lower(budget_tier)                                        as budget_tier,
    lower(bid_strategy)                                       as bid_strategy,
    daily_budget_usd,
    convert_timezone('UTC', changed_at)::timestamp_ntz        as changed_at,
    change_reason
from {{ source('criteo', 'campaign_changes') }}
-- if the upstream system ever emits two states at the same instant, keep the last loaded
qualify row_number() over (partition by campaign_id, changed_at order by _loaded_at desc) = 1
