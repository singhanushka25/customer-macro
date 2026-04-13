{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
    database=var('src_database'),
    schema=var('src_schema')
) }}

-- Post-load equivalent of a pre-load Python `transform(event)`.
--
-- Runtime inputs (required; pass via `--vars`):
--   src_database : database holding the source table (PLT_final lands here too)
--   src_schema   : schema holding the source table (also where PLT_final lands)
--   src_table    : name of the source table to transform
--
-- Usage:
--   dbt run --profiles-dir . --select PLT_final \
--     --vars '{"src_database": "MY_DB", "src_schema": "MY_SCHEMA", "src_table": "events"}'
--
-- Original pre-load logic (per-row, Python):
--   if eventName == '<schema>.<table>':
--       if properties['phone_no_shared'] == 0: properties['phone_no_shared'] = False
--       elif properties['phone_no_shared'] == 1: properties['phone_no_shared'] = True
--
-- Set-based equivalent:
--   0     -> FALSE
--   1     -> TRUE
--   NULL  -> NULL (CASE propagates NULL naturally)
--   other -> NULL
--
-- Incremental strategy:
--   * `unique_key='id'` — source guarantees `id` is the PK. dbt builds a MERGE
--     on id, so updates to an existing row replace the target row in place.
--   * Watermark: `__HEVO__LOADED_AT` — ingestion timestamp bumped on insert/update.
--   * Idempotent: SELECT is a pure function of the source row, so overlapping
--     windows or re-runs produce identical target state.
--
-- Target:
--   * Table name   : PLT_final (derived from this file's name)
--   * Schema       : `var('src_schema')` via `config(schema=...)`, using the
--                    override in `macros/generate_schema_name.sql` so the schema
--                    is used verbatim (not suffixed to `target.schema`).
--   * Database     : `var('src_database')` — overrides `target.database` so a
--                    single profile can target multiple databases at run time.

{% if is_incremental() %}
    -- Incremental step — runs ONLY after the table has been seeded.
    -- Rows arriving post-migration come from the edge pipeline without the
    -- pre-load Python transformation, so `phone_no_shared` is still the raw
    -- '0'/'1' value. Apply the same mapping the Python script used
    -- (`'0'` -> `'False'`, `'1'` -> `'True'`) so merged rows are
    -- indistinguishable from the pre-migration rows already in the table.
    select
        * exclude (phone_no_shared),
        case
            when phone_no_shared = '0' then 'False'
            when phone_no_shared = '1' then 'True'
        end::varchar as phone_no_shared
    from {{ var('src_database') }}.{{ var('src_schema') }}.{{ var('src_table') }}
    where "__HEVO__LOADED_AT" > (select coalesce(max("__HEVO__LOADED_AT"), 0) from {{ this }})
{% else %}
    -- First run (seed) — the source already contains the Python-transformed
    -- values ('True'/'False') from the standard pipeline, so a plain SELECT *
    -- preserves them as-is. No CASE here; applying it would blow away the
    -- already-transformed values with NULL.
    select *
    from {{ var('src_database') }}.{{ var('src_schema') }}.{{ var('src_table') }}
{% endif %}
