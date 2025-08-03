-- Pivot macro for creating dynamic pivot tables
-- Usage: {{ pivot('category_column', ['value1', 'value2'], 'sum') }}

{% macro pivot(column, values, agg='sum', quote_identifiers=true) %}
    {%- for value in values %}
        {{ agg }}(case when {{ column }} = '{{ value }}' then 1 else 0 end) 
        as {{ value | replace(' ', '_') | replace('-', '_') | lower }}
        {%- if not loop.last %},{% endif -%}
    {%- endfor %}
{% endmacro %}

-- Example usage in a model:
-- select 
--     customer_id,
--     {{ pivot('product_category', ['electronics', 'clothing', 'books'], 'count') }}
-- from {{ ref('order_items') }}
-- group by customer_id