{#
  Post-hook: for every column documented with `meta: {pii: <type>}`, set the
  Snowflake tag governance.tags.pii_type = <type>. The masking policy is attached
  to that tag (include/snowflake/02_governance.sql), so tagging = masking.

  Why a hook: `create or replace table` drops column tags, so they must be
  re-applied on every build. Driving it from YAML meta keeps the classification
  next to the column documentation that dbt docs publishes.
#}
{% macro apply_pii_tags() %}
  {%- if execute and model.columns -%}
    {%- set relation_type = 'view' if model.config.materialized == 'view' else 'table' -%}
    {%- set stmts = [] -%}
    {%- for col_name, col in model.columns.items() -%}
      {%- set meta = col.meta or {} -%}
      {%- set cfg_meta = (col.config or {}).get('meta', {}) if col.config is mapping else {} -%}
      {%- set pii = meta.get('pii') or cfg_meta.get('pii') -%}
      {%- if pii -%}
        {%- do stmts.append(
              "alter " ~ relation_type ~ " " ~ this ~ " modify column " ~ adapter.quote(col_name | upper)
              ~ " set tag " ~ var('pii_tag') ~ " = '" ~ pii ~ "'") -%}
      {%- endif -%}
    {%- endfor -%}
    {#- one statement per call: Snowflake rejects multi-statement strings by default -#}
    {%- for s in stmts -%}
      {%- do run_query(s) -%}
      {%- do log("pii tag: " ~ s, info=false) -%}
    {%- endfor -%}
    select 1 /* {{ stmts | length }} pii column(s) tagged */
  {%- else -%}
    select 1
  {%- endif -%}
{% endmacro %}
