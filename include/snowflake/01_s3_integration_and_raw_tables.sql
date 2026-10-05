-- =============================================================================
-- 01 – S3 storage integration, stages and RAW tables
--   snow sql -f include/snowflake/01_s3_integration_and_raw_tables.sql \
--     -D "s3_bucket=my-bucket" -D "aws_role_arn=arn:aws:iam::123456789012:role/snowflake-criteo-reader"
--
-- After running, do the IAM trust step in README ("Connect Snowflake to S3"):
--   desc integration s3_criteo_int;  -> copy STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID
--   into the IAM role's trust policy.
-- =============================================================================

use role accountadmin;

create storage integration if not exists s3_criteo_int
  type = external_stage
  storage_provider = 'S3'
  enabled = true
  storage_aws_role_arn = '<% aws_role_arn %>'
  -- Snowflake can only ever read the cleaned output and the campaign change log
  storage_allowed_locations = (
    's3://<% s3_bucket %>/clean/criteo/',
    's3://<% s3_bucket %>/raw/criteo/campaign_changes/'
  );

grant usage on integration s3_criteo_int to role loader;

-- ---------- grants so LOADER owns RAW objects ---------------------------------
grant usage on database raw to role loader;
grant usage, create table, create stage, create file format on schema raw.criteo to role loader;
grant usage on warehouse loading_wh to role loader;

use role loader;
use warehouse loading_wh;
use schema raw.criteo;

-- use_logical_type: read Parquet TIMESTAMP/DECIMAL logical types as such, not raw ints
create file format if not exists ff_parquet type = parquet use_logical_type = true;
create file format if not exists ff_csv
  type = csv skip_header = 1 field_optionally_enclosed_by = '"' null_if = ('');

create stage if not exists clean_stage
  storage_integration = s3_criteo_int
  url = 's3://<% s3_bucket %>/clean/criteo/'
  file_format = ff_parquet;

create stage if not exists campaign_changes_stage
  storage_integration = s3_criteo_int
  url = 's3://<% s3_bucket %>/raw/criteo/campaign_changes/'
  file_format = ff_csv;

-- FALLBACK if `list @clean_stage` fails with "not authorized to perform:
-- sts:AssumeRole" even though the trust policy is correct (some AWS accounts,
-- e.g. organization-managed ones, block cross-account role assumption):
-- create an IAM user with ONLY the read policy for these two prefixes, then:
--   create or replace stage clean_stage
--     url = 's3://<% s3_bucket %>/clean/criteo/'
--     credentials = (aws_key_id = '...' aws_secret_key = '...')
--     file_format = ff_parquet;
--   create or replace stage campaign_changes_stage
--     url = 's3://<% s3_bucket %>/raw/criteo/campaign_changes/'
--     credentials = (aws_key_id = '...' aws_secret_key = '...')
--     file_format = ff_csv;
-- Snowflake encrypts stage credentials and never shows them back.

-- One row per cleaned impression. Spark output columns + load metadata.
create table if not exists impressions (
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
  raw_source_file           varchar,
  dt                        date           not null,
  _stage_file               varchar,
  _loaded_at                timestamp_ltz  not null default current_timestamp()
)
cluster by (dt)
comment = 'Cleaned Criteo impressions loaded from S3 Parquet. One partition (dt) reloaded per Airflow run.';

-- Simulated campaign attribute change log (source for dim_campaign SCD2)
create table if not exists campaign_changes (
  campaign_id        number(38,0)  not null,
  advertiser_id      number(38,0),
  campaign_vertical  varchar,
  budget_tier        varchar,
  bid_strategy       varchar,
  daily_budget_usd   number(12,2),
  changed_at         timestamp_tz  not null,
  change_reason      varchar,
  _loaded_at         timestamp_ltz not null default current_timestamp()
);

-- dbt's TRANSFORMER role reads RAW, never writes it
use role accountadmin;
grant usage on database raw to role transformer;
grant usage on schema raw.criteo to role transformer;
grant select on all tables in schema raw.criteo to role transformer;
grant select on future tables in schema raw.criteo to role transformer;
