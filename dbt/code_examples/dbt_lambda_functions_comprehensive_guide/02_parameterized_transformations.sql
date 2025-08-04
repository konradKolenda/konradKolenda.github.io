-- 02_parameterized_transformations.sql
-- Advanced parameterized lambda-like functions

-- Generic transformation macro with multiple types
{% macro transform_column(column_name, transformation_type, params={}) %}
    {% if transformation_type == 'normalize' %}
        -- Min-max normalization
        ({{ column_name }} - {{ params.get('min', 0) }}) / 
        ({{ params.get('max', 1) }} - {{ params.get('min', 0) }})
    {% elif transformation_type == 'standardize' %}
        -- Z-score standardization
        ({{ column_name }} - {{ params.get('mean', 0) }}) / {{ params.get('std', 1) }}
    {% elif transformation_type == 'log' %}
        -- Logarithmic transformation
        {% if params.get('base') == 'e' %}
            ln({{ column_name }})
        {% else %}
            log({{ params.get('base', 10) }}, {{ column_name }})
        {% endif %}
    {% elif transformation_type == 'scale' %}
        -- Simple scaling
        {{ column_name }} * {{ params.get('factor', 1) }}
    {% elif transformation_type == 'cap' %}
        -- Cap values at min/max
        greatest({{ params.get('min', 0) }}, 
                least({{ params.get('max', 100) }}, {{ column_name }}))
    {% else %}
        {{ column_name }}
    {% endif %}
{% endmacro %}

-- Conditional transformation based on another column
{% macro conditional_transform(value_column, condition_column, condition_value, transform_true, transform_false) %}
    case 
        when {{ condition_column }} = '{{ condition_value }}' then {{ transform_true }}
        else {{ transform_false }}
    end
{% endmacro %}

-- Multi-step transformation pipeline
{% macro pipeline_transform(column_name, steps) %}
    {% set result = column_name %}
    {% for step in steps %}
        {% set result = transform_column(result, step.type, step.params) %}
    {% endfor %}
    {{ result }}
{% endmacro %}

-- Usage examples
select 
    customer_id,
    
    -- Normalize revenue to 0-1 scale
    {{ transform_column('revenue', 'normalize', {'min': 0, 'max': 10000}) }} as revenue_normalized,
    
    -- Standardize customer score
    {{ transform_column('customer_score', 'standardize', {'mean': 50, 'std': 15}) }} as score_standardized,
    
    -- Log transform for skewed data
    {{ transform_column('page_views', 'log', {'base': 'e'}) }} as log_page_views,
    
    -- Scale and cap discount percentage
    {{ transform_column('raw_discount', 'cap', {'min': 0, 'max': 50}) }} as discount_percent,
    
    -- Conditional transformation based on customer type
    {{ conditional_transform('base_price', 'customer_type', 'premium', 
        transform_column('base_price', 'scale', {'factor': 0.9}), 
        'base_price') }} as final_price,
    
    order_date
from {{ ref('customer_orders') }}