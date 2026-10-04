-- =============================================================================
-- 00 – Warehouses, databases, roles, service user
-- Run as ACCOUNTADMIN (one time):
--   snow sql -f include/snowflake/00_account_setup.sql -D "svc_public_key=MIIBIjANBg..."
-- Requires an ENTERPRISE edition account (choose it when creating the free trial):
-- masking policies and tag-based masking are Enterprise features.
-- =============================================================================

use role accountadmin;

-- ---------- warehouses: one per workload so cost is attributable --------------
create warehouse if not exists loading_wh
  warehouse_size = xsmall auto_suspend = 60 auto_resume = true initially_suspended = true
  comment = 'COPY INTO from S3';
create warehouse if not exists transforming_wh
  warehouse_size = xsmall auto_suspend = 60 auto_resume = true initially_suspended = true
  comment = 'dbt builds';
create warehouse if not exists reporting_wh
  warehouse_size = xsmall auto_suspend = 60 auto_resume = true initially_suspended = true
  comment = 'BI / analyst queries';

-- guardrail so a runaway query can't burn the trial credits
create resource monitor if not exists ad_analytics_monitor
  with credit_quota = 50 frequency = monthly start_timestamp = immediately
  triggers on 80 percent do notify
           on 100 percent do suspend;
alter warehouse loading_wh      set resource_monitor = ad_analytics_monitor;
alter warehouse transforming_wh set resource_monitor = ad_analytics_monitor;
alter warehouse reporting_wh    set resource_monitor = ad_analytics_monitor;

-- ---------- databases ---------------------------------------------------------
create database if not exists raw        comment = 'Landed data, written only by LOADER';
create schema   if not exists raw.criteo;
create database if not exists analytics  comment = 'dbt-built models, written only by TRANSFORMER';
create database if not exists governance comment = 'Policies and tags, owned by GOVERNANCE_ADMIN';
create schema   if not exists governance.policies;
create schema   if not exists governance.tags;

-- ---------- roles (functional, least privilege) -------------------------------
create role if not exists loader           comment = 'Loads RAW from S3';
create role if not exists transformer      comment = 'Runs dbt: reads RAW, writes ANALYTICS';
create role if not exists analyst          comment = 'Reads ANALYTICS core + marts; user_id masked';
create role if not exists pii_reader       comment = 'Sees unmasked user_id. Grant to named humans only.';
create role if not exists governance_admin comment = 'Owns masking policies and tags';

-- everything rolls up to SYSADMIN so admins can manage objects ...
grant role loader           to role sysadmin;
grant role transformer      to role sysadmin;
grant role analyst          to role sysadmin;
grant role governance_admin to role sysadmin;
-- ... except PII_READER, which deliberately does NOT roll up.

-- ---------- service user for Airflow + dbt (key-pair auth) --------------------
-- Generate keys:  openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
--                 openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
-- Pass the base64 body of rsa_key.pub (no header/footer lines) as svc_public_key.
create user if not exists airflow_svc
  type = service
  default_role = transformer
  default_warehouse = transforming_wh
  rsa_public_key = '<% svc_public_key %>'
  comment = 'Airflow + dbt service account';
grant role loader      to user airflow_svc;
grant role transformer to user airflow_svc;

-- your own human user: lets you switch into any of the functional roles
set my_user = current_user();
grant role analyst          to user identifier($my_user);
grant role governance_admin to user identifier($my_user);
-- grant role pii_reader to user identifier($my_user);   -- only to demo unmasking
