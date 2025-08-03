-- Custom schema name generation macro (overrides built-in macro)
-- This macro customizes how dbt generates schema names

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ default_schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{% endmacro %}

-- Usage: Set schema in model config
-- {{ config(schema='staging') }}
-- This will create tables in schema: "analytics_staging" (if target.schema = "analytics")

-- Alternative pattern for environment-based schemas:
{% macro generate_schema_name_alt(custom_schema_name, node) -%}
    {%- if target.name == 'prod' and custom_schema_name is not none -%}
        {{ custom_schema_name | trim }}
    {%- elif custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ target.schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{% endmacro %}