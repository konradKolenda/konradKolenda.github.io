-- Surrogate key generation macro
-- Usage: {{ surrogate_key(['col1', 'col2', 'col3']) }}

{% macro surrogate_key(field_list) %}
    md5(concat(
        {%- for field in field_list %}
            coalesce(cast({{ field }} as string), '')
            {%- if not loop.last %}, '|', {% endif -%}
        {%- endfor %}
    ))
{% endmacro %}

-- Example usage in a model:
-- select 
--     {{ surrogate_key(['customer_id', 'product_id', 'order_date']) }} as dim_key,
--     customer_id,
--     product_id,
--     order_date,
--     quantity,
--     price
-- from {{ source('raw', 'order_items') }}