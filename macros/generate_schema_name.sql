{#
    Override dbt's default `generate_schema_name` so any `{{ config(schema=...) }}`
    value is used verbatim — NOT appended to `target.schema`. This lets the
    PLT_final model materialize into the exact schema the user passes via
    `--vars "{src_schema: <name>, src_table: <name>}"`.

    Without this override, dbt would build the table in `<target.schema>_<custom>`
    which breaks the "same schema as source" requirement for the POC.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
