{#
    Override dbt's default `generate_database_name` so any `{{ config(database=...) }}`
    value is used verbatim — NOT the profile's `target.database`. This lets the
    PLT_final model materialize into the exact database the user passes via
    `--vars "{src_database: <name>, ...}"`.

    Without this override, dbt uses `target.database` from the profile even when
    `config(database=...)` is set, which breaks the "same DB as source" requirement.
#}
{% macro generate_database_name(custom_database_name, node) -%}
    {%- if custom_database_name is none -%}
        {{ target.database }}
    {%- else -%}
        {{ custom_database_name | trim }}
    {%- endif -%}
{%- endmacro %}
