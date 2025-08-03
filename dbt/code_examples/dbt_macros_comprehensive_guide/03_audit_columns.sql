-- Audit columns macro for adding metadata to models
-- Usage: {{ audit_columns() }} at the end of your select statement

{% macro audit_columns() %}
    '{{ target.name }}' as target_name,
    '{{ model.name }}' as model_name,
    current_timestamp() as dbt_updated_at,
    '{{ invocation_id }}' as dbt_invocation_id
{% endmacro %}

-- Example usage in a model:
-- select 
--     order_id,
--     customer_id,
--     order_date,
--     total_amount,
--     {{ audit_columns() }}
-- from {{ source('raw', 'orders') }}