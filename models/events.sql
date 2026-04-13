{{ config(materialized='view') }}

-- Post-load equivalent of a pre-load Python `transform(event)`.
--
-- Original pre-load logic (per-row, Python):
--   if eventName == '<schema>.<table>':
--       if properties['phone_no_shared'] == 0: properties['phone_no_shared'] = False
--       elif properties['phone_no_shared'] == 1: properties['phone_no_shared'] = True
--   # missing key / other value / NULL → passthrough
--
-- Set-based equivalent (runs once over the whole table, no per-row overhead):
--   0     -> FALSE
--   1     -> TRUE
--   NULL  -> NULL (CASE propagates NULL naturally)
--   other -> NULL   (sentinel; in practice this column is only 0/1/NULL)
--
-- Notes:
--   * The Python `eventName == '...'` guard is implicit — this model *is* the
--     transform for that one table, applied only to rows from that source.
--   * `SELECT * EXCLUDE (...)` is Snowflake/BigQuery syntax. For Redshift,
--     replace with an explicit column list or `dbt_utils.star(except=[...])`.

select
    * exclude (phone_no_shared),
    case
        when phone_no_shared = 0 then false
        when phone_no_shared = 1 then true
    end as phone_no_shared
from {{ source('raw', 'events') }}
