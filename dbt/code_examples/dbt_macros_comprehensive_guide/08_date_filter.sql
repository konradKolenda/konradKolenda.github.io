-- Dynamic date filter macro
-- Usage: where {{ date_filter('created_at', var('start_date'), var('end_date')) }}

{% macro date_filter(date_column, start_date=none, end_date=none) %}
    {%- if start_date and end_date -%}
        {{ date_column }} between '{{ start_date }}' and '{{ end_date }}'
    {%- elif start_date -%}
        {{ date_column }} >= '{{ start_date }}'
    {%- elif end_date -%}
        {{ date_column }} <= '{{ end_date }}'
    {%- else -%}
        1=1
    {%- endif -%}
{% endmacro %}

-- Example usage in a model:
-- select *
-- from {{ source('raw', 'orders') }}
-- where {{ date_filter('order_date', var('start_date', none), var('end_date', none)) }}

-- Run with variables:
-- dbt run --vars '{"start_date": "2024-01-01", "end_date": "2024-12-31"}'