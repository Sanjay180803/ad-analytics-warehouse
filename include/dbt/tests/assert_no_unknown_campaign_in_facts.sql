{{ config(severity='warn') }}
-- Every impression should resolve to a real campaign version. Spark already
-- quarantines unknown campaign IDs, so any hit here means the as-of join missed
-- (e.g. an impression earlier than the campaign's first version).
select impression_id, campaign_id, event_ts
from {{ ref('fct_impressions') }}
where campaign_sk = '-1'
