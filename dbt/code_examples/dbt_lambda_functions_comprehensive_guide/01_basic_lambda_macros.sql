-- 01_basic_lambda_macros.sql
-- Basic lambda-like functions using dbt macros

-- Simple transformation macro (lambda-like)
{% macro to_dollars(column_name, scale=2) %}
    ({{ column_name }} / 100)::numeric(16, {{ scale }})
{% endmacro %}

-- Safe division macro (handles edge cases)
{% macro safe_divide(numerator, denominator, default_value=0) %}
    case 
        when {{ denominator }} = 0 or {{ denominator }} is null then {{ default_value }}
        else {{ numerator }} / {{ denominator }}
    end
{% endmacro %}

-- Boolean conversion macro
{% macro to_boolean(column_name, true_values=['1', 'true', 'yes', 'y']) %}
    case 
        when lower({{ column_name }}::text) in ({{ true_values | map('lower') | join(', ') }}) then true
        else false
    end
{% endmacro %}

-- Usage examples in a model
select 
    order_id,
    customer_id,
    
    -- Convert cents to dollars
    {{ to_dollars('amount_cents') }} as amount_dollars,
    
    -- Safe division for calculating unit price
    {{ safe_divide('total_amount', 'quantity', 'null') }} as unit_price,
    
    -- Convert string flags to boolean
    {{ to_boolean('is_premium', ['1', 'true', 'premium', 'yes']) }} as is_premium_customer,
    
    order_date
from {{ ref('raw_orders') }}