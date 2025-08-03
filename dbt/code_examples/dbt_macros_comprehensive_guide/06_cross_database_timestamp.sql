-- Cross-database compatible timestamp macro
-- Usage: {{ current_timestamp_tz() }}

{% macro current_timestamp_tz() %}
    {%- if target.type == 'snowflake' -%}
        current_timestamp()
    {%- elif target.type == 'bigquery' -%}
        current_timestamp()
    {%- elif target.type == 'postgres' -%}
        now()
    {%- elif target.type == 'redshift' -%}
        getdate()
    {%- else -%}
        current_timestamp()
    {%- endif -%}
{% endmacro %}

-- Example usage in a model:
-- select 
--     order_id,
--     customer_id,
--     {{ current_timestamp_tz() }} as processed_at
-- from {{ source('raw', 'orders') }}