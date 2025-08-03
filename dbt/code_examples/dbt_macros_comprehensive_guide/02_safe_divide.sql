-- Safe division macro that handles division by zero
-- Usage: {{ safe_divide('numerator_col', 'denominator_col') }}

{% macro safe_divide(numerator, denominator) %}
    case
        when {{ denominator }} = 0 then null
        else {{ numerator }} / {{ denominator }}
    end
{% endmacro %}

-- Example usage in a model:
-- select 
--     customer_id,
--     total_orders,
--     total_revenue,
--     {{ safe_divide('total_revenue', 'total_orders') }} as avg_order_value
-- from {{ ref('customer_summary') }}