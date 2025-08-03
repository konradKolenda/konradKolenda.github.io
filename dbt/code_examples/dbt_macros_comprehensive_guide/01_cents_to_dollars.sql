-- Basic utility macro for converting cents to dollars
-- Usage: {{ cents_to_dollars('price_cents', 2) }}

{% macro cents_to_dollars(column_name, scale=2) %}
    ({{ column_name }} / 100)::numeric(16, {{ scale }})
{% endmacro %}

-- Example usage in a model:
-- select 
--     order_id,
--     {{ cents_to_dollars('price_in_cents') }} as price_in_dollars,
--     {{ cents_to_dollars('tax_in_cents', 4) }} as tax_in_dollars
-- from {{ source('raw', 'orders') }}