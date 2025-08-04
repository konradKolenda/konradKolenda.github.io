-- 03_higher_order_functions.sql
-- Higher-order functions that accept other functions as parameters

-- Apply a transformation to multiple columns
{% macro apply_to_columns(columns, transform_macro, params={}) %}
    {% for column in columns %}
        {{ transform_macro(column, **params) }} as {{ column }}_{{ transform_macro.split('(')[0] }}
        {%- if not loop.last -%}, {% endif %}
    {% endfor %}
{% endmacro %}

-- Map function over a list of columns
{% macro map_columns(columns, func_name, suffix='_mapped') %}
    {% for col in columns %}
        {{ func_name }}({{ col }}) as {{ col }}{{ suffix }}
        {%- if not loop.last -%}, {% endif %}
    {% endfor %}
{% endmacro %}

-- Filter columns based on a condition function
{% macro filter_and_select(all_columns, condition_func) %}
    {% set filtered_columns = [] %}
    {% for col in all_columns %}
        {% if condition_func(col) %}
            {% set _ = filtered_columns.append(col) %}
        {% endif %}
    {% endfor %}
    {{ filtered_columns | join(', ') }}
{% endmacro %}

-- Reduce multiple columns using an operation
{% macro reduce_columns(columns, operation='sum', alias='total') %}
    {% if operation == 'sum' %}
        ({{ columns | join(' + ') }}) as {{ alias }}
    {% elif operation == 'avg' %}
        ({{ columns | join(' + ') }}) / {{ columns | length }} as {{ alias }}
    {% elif operation == 'max' %}
        greatest({{ columns | join(', ') }}) as {{ alias }}
    {% elif operation == 'min' %}
        least({{ columns | join(', ') }}) as {{ alias }}
    {% elif operation == 'concat' %}
        concat({{ columns | join(', ') }}) as {{ alias }}
    {% endif %}
{% endmacro %}

-- Compose two functions
{% macro compose(f1, f2, column_name, f1_params={}, f2_params={}) %}
    {{ f1(f2(column_name, **f2_params), **f1_params) }}
{% endmacro %}

-- Function factory pattern
{% macro create_validator(validation_type) %}
    {% if validation_type == 'email' %}
        {% set pattern = '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$' %}
        regexp_like({{ column }}, '{{ pattern }}')
    {% elif validation_type == 'phone' %}
        length(regexp_replace({{ column }}, '[^0-9]', '', 'g')) >= 10
    {% elif validation_type == 'url' %}
        {{ column }} ~ '^https?://.+'
    {% elif validation_type == 'positive' %}
        {{ column }} > 0
    {% endif %}
{% endmacro %}

-- Usage examples
with base_data as (
    select 
        customer_id,
        revenue_q1,
        revenue_q2, 
        revenue_q3,
        revenue_q4,
        email,
        phone,
        website
    from {{ ref('customer_data') }}
)

select 
    customer_id,
    
    -- Apply safe_divide to multiple revenue columns
    {{ apply_to_columns(['revenue_q1', 'revenue_q2', 'revenue_q3', 'revenue_q4'], 
                        'safe_divide', {'denominator': '4', 'default_value': '0'}) }},
    
    -- Reduce quarterly revenues to total and average
    {{ reduce_columns(['revenue_q1', 'revenue_q2', 'revenue_q3', 'revenue_q4'], 
                      'sum', 'total_revenue') }},
    {{ reduce_columns(['revenue_q1', 'revenue_q2', 'revenue_q3', 'revenue_q4'], 
                      'avg', 'avg_quarterly_revenue') }},
    
    -- Validate contact information
    {{ create_validator('email').replace('{{ column }}', 'email') }} as valid_email,
    {{ create_validator('phone').replace('{{ column }}', 'phone') }} as valid_phone,
    {{ create_validator('url').replace('{{ column }}', 'website') }} as valid_website
    
from base_data