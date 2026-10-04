-- Full reload of the (small) campaign change log. Idempotent.
begin;

truncate table raw.criteo.campaign_changes;

copy into raw.criteo.campaign_changes (
  campaign_id, advertiser_id, campaign_vertical, budget_tier, bid_strategy,
  daily_budget_usd, changed_at, change_reason
)
from (
  select $1::number, $2::number, $3, $4, $5, $6::number(12,2),
         $7::timestamp_tz, $8
  from @raw.criteo.campaign_changes_stage/campaign_changes.csv
)
file_format = (format_name = raw.criteo.ff_csv)
force = true
on_error = abort_statement;

commit;
