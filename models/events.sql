{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='merge',
    on_schema_change='append_new_columns'
) }}

-- Post-load equivalent of a pre-load Python `transform(event)`.
--
-- Original pre-load logic (per-row, Python):
--   if eventName == '<schema>.<table>':
--       if properties['phone_no_shared'] == 0: properties['phone_no_shared'] = False
--       elif properties['phone_no_shared'] == 1: properties['phone_no_shared'] = True
--   # missing key / other value / NULL → passthrough
--
-- Set-based equivalent (runs once per dbt run, no per-row overhead):
--   0     -> FALSE
--   1     -> TRUE
--   NULL  -> NULL (CASE propagates NULL naturally)
--   other -> NULL   (sentinel; in practice this column is only 0/1/NULL)
--
-- Incremental strategy:
--   * `unique_key='id'`         — source guarantees `id` is the PK. dbt builds a
--                                  MERGE on id, so updates to an existing row
--                                  replace the target row in place.
--   * Watermark: `_hevo_loaded_at` — ingestion timestamp bumped on insert/update.
--   * Idempotent by construction: SELECT is a pure function of the source row,
--     so overlapping windows or re-runs produce identical target state.
--
-- Notes:
--   * The Python `eventName == '...'` guard is implicit — this model *is* the
--     transform for that one table, applied only to rows from that source.
--   * `SELECT * EXCLUDE (...)` is Snowflake/BigQuery syntax. For Redshift,
--     replace with an explicit column list or `dbt_utils.star(except=[...])`.
--   * `on_schema_change='append_new_columns'` lets upstream column additions
--     flow through without a manual --full-refresh.

select
    * exclude (phone_no_shared),
    case
        when phone_no_shared = 0 then false
        when phone_no_shared = 1 then true
    end as phone_no_shared
from {{ source('raw', 'events') }}

{% if is_incremental() %}
    -- Only process rows loaded/updated since the last successful run.
    -- On first run (target doesn't exist) this block is skipped and the
    -- model does a full build.
    where _hevo_loaded_at > (select coalesce(max(_hevo_loaded_at), '1900-01-01'::timestamp) from {{ this }})
{% endif %}
