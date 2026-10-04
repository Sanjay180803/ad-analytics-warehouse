-- =============================================================================
-- 02 – Governance: tag-based masking of user IDs + role-based access
--   snow sql -f include/snowflake/02_governance.sql -D "hash_salt=<random 32+ chars>"
--
-- Design:
--   * One tag, governance.tags.pii_type. Classifying a column = setting the tag.
--   * The masking policy is attached to the TAG, not to columns. Any string column
--     tagged pii_type = 'user_id' is masked automatically, including every table
--     dbt rebuilds (dbt re-applies the tag in a post-hook; see macros/apply_pii_tags.sql).
--   * Analysts get a salted SHA-256 pseudonym, not NULL or '***'. They can still
--     count distinct users and join facts on user_id, but cannot recover the ID.
--   * PII_READER sees the real value. TRANSFORMER/LOADER see it because they must
--     move it through the pipeline; nobody queries as those roles interactively.
-- =============================================================================

use role accountadmin;
grant ownership on schema governance.policies to role governance_admin copy current grants;
grant ownership on schema governance.tags     to role governance_admin copy current grants;
grant usage on database governance to role governance_admin;
grant apply masking policy on account to role governance_admin;
grant apply tag on account to role governance_admin;

use role governance_admin;

create tag if not exists governance.tags.pii_type
  allowed_values 'user_id', 'email', 'ip_address'
  comment = 'PII classification. Masking policies attach to this tag.';

create masking policy if not exists governance.policies.mask_user_id
  as (val string) returns string ->
  case
    when val is null then null
    when is_role_in_session('PII_READER')  then val
    when is_role_in_session('TRANSFORMER') then val
    when is_role_in_session('LOADER')      then val
    else sha2(val || '<% hash_salt %>', 256)
  end
  comment = 'Pseudonymise user IDs for everyone except PII_READER and pipeline roles';

-- attach once to the tag; every tagged column inherits it
alter tag governance.tags.pii_type set masking policy governance.policies.mask_user_id;

-- classify the RAW column
use role accountadmin;
alter table raw.criteo.impressions modify column user_id
  set tag governance.tags.pii_type = 'user_id';

-- dbt (TRANSFORMER) must be able to tag the columns of tables it creates
grant usage on database governance to role transformer;
grant usage on schema governance.tags to role transformer;
use role governance_admin;
grant apply on tag governance.tags.pii_type to role transformer;

-- ---------- ANALYTICS access ---------------------------------------------------
use role accountadmin;
grant usage on warehouse transforming_wh to role transformer;
grant usage, create schema on database analytics to role transformer;

grant usage on warehouse reporting_wh to role analyst;
grant usage on database analytics to role analyst;
-- schema + table SELECT grants for ANALYST are issued by dbt (`grants:` config in
-- dbt_project.yml), so access lives in version control next to the models.

grant role analyst to role pii_reader;   -- PII readers can do everything analysts can

-- ---------- verification (run after the first dbt build) ----------------------
-- use role analyst;    select user_id from analytics.core.fct_impressions limit 5;  -- 64-char hashes
-- use role pii_reader; select user_id from analytics.core.fct_impressions limit 5;  -- real IDs
-- select * from table(analytics.information_schema.policy_references(
--     ref_entity_name => 'analytics.core.fct_impressions', ref_entity_domain => 'table'));
-- select * from snowflake.account_usage.tag_references where tag_name = 'PII_TYPE';  -- ~2h latency
