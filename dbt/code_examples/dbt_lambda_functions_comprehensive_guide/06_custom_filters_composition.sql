-- 06_custom_filters_composition.sql
-- Custom filters and function composition patterns

-- String transformation filters
{% macro snake_case(text) %}
    {{ text | lower | replace(' ', '_') | replace('-', '_') | regex_replace('[^a-z0-9_]', '') }}
{% endmacro %}

{% macro pascal_case(text) %}
    {{ text | title | replace(' ', '') | replace('_', '') | replace('-', '') }}
{% endmacro %}

{% macro clean_column_name(column_name) %}
    {{ snake_case(column_name) | truncate(63, true, '') }}
{% endmacro %}

-- Numeric transformation filters
{% macro round_to_precision(value, precision=2) %}
    round({{ value }}::numeric, {{ precision }})
{% endmacro %}

{% macro percentage_format(value, precision=1) %}
    {{ round_to_precision(value * 100, precision) }}
{% endmacro %}

{% macro currency_format(value, precision=2) %}
    '$' || {{ round_to_precision(value, precision) }}
{% endmacro %}

-- Date transformation filters
{% macro format_date(date_column, format_string='YYYY-MM-DD') %}
    to_char({{ date_column }}, '{{ format_string }}')
{% endmacro %}

{% macro age_in_days(date_column, reference_date='current_date') %}
    {{ reference_date }} - {{ date_column }}
{% endmacro %}

{% macro date_bucket(date_column, bucket_type='month') %}
    {% if bucket_type == 'month' %}
        date_trunc('month', {{ date_column }})
    {% elif bucket_type == 'quarter' %}
        date_trunc('quarter', {{ date_column }})
    {% elif bucket_type == 'year' %}
        date_trunc('year', {{ date_column }})
    {% elif bucket_type == 'week' %}
        date_trunc('week', {{ date_column }})
    {% else %}
        date_trunc('day', {{ date_column }})
    {% endif %}
{% endmacro %}

-- Function composition utilities
{% macro compose_two(f1, f2, input, f1_params={}, f2_params={}) %}
    {{ f1(f2(input, **f2_params), **f1_params) }}
{% endmacro %}

{% macro compose_multiple(functions, input) %}
    {% set result = input %}
    {% for func in functions %}
        {% set result = func.name(result, **func.get('params', {})) %}
    {% endfor %}
    {{ result }}
{% endmacro %}

-- Functional validation chain
{% macro validation_chain(column_name, validators) %}
    {% set conditions = [] %}
    {% for validator in validators %}
        {% set condition = validator.condition.replace('VALUE', column_name) %}
        {% set _ = conditions.append('(' ~ condition ~ ')') %}
    {% endfor %}
    {{ conditions | join(' and ') }}
{% endmacro %}

-- Custom aggregation filters
{% macro weighted_average(value_column, weight_column) %}
    sum({{ value_column }} * {{ weight_column }}) / sum({{ weight_column }})
{% endmacro %}

{% macro percentile(column_name, percentile_value) %}
    percentile_cont({{ percentile_value }}) within group (order by {{ column_name }})
{% endmacro %}

{% macro moving_average(column_name, window_size=7) %}
    avg({{ column_name }}) over (
        rows between {{ window_size - 1 }} preceding and current row
    )
{% endmacro %}

-- Usage examples demonstrating filter composition

-- Example 1: String processing pipeline
{% set string_processors = [
    {'name': 'lower'},
    {'name': 'trim'},
    {'name': 'replace', 'params': {'old': ' ', 'new': '_'}},
    {'name': 'regex_replace', 'params': {'pattern': '[^a-z0-9_]', 'replacement': ''}}
] %}

select 
    customer_id,
    
    -- Composed string transformations
    {{ snake_case('customer_name') }} as customer_name_clean,
    {{ pascal_case('product_category') }} as product_category_formatted,
    {{ clean_column_name('messy column name!@#') }} as clean_column,
    
    -- Numeric transformations with composition
    {{ currency_format(percentage_format('discount_rate', 1), 2) }} as discount_display,
    {{ round_to_precision('revenue * tax_rate', 2) }} as tax_amount,
    
    -- Date transformations
    {{ format_date('order_date', 'Month DD, YYYY') }} as formatted_order_date,
    {{ age_in_days('order_date') }} as days_since_order,
    {{ date_bucket('order_date', 'quarter') }} as order_quarter

from {{ ref('raw_customer_orders') }}

-- Example 2: Validation chain composition
{% set email_validators = [
    {'condition': 'VALUE is not null', 'message': 'Email cannot be null'},
    {'condition': 'length(VALUE) > 5', 'message': 'Email too short'},
    {'condition': 'VALUE ~ \'@\'', 'message': 'Email must contain @'},
    {'condition': 'VALUE ~ \'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$\'', 'message': 'Invalid email format'}
] %}

select 
    customer_id,
    email,
    {{ validation_chain('email', email_validators) }} as email_is_valid
from {{ ref('customers') }}

-- Example 3: Advanced aggregation with custom filters
select 
    product_category,
    
    -- Standard aggregations
    count(*) as order_count,
    sum(revenue) as total_revenue,
    avg(revenue) as avg_revenue,
    
    -- Custom aggregations using filters
    {{ weighted_average('rating', 'review_count') }} as weighted_avg_rating,
    {{ percentile('revenue', 0.5) }} as median_revenue,
    {{ percentile('revenue', 0.95) }} as p95_revenue,
    
    -- Moving averages (when used in window context)
    {{ moving_average('daily_revenue', 30) }} as revenue_30day_ma

from {{ ref('product_performance') }}
group by product_category

-- Example 4: Complex function composition for derived metrics
{% macro customer_score(recency_days, frequency_orders, monetary_value) %}
    {% set recency_score %}
        case 
            when {{ recency_days }} <= 30 then 5
            when {{ recency_days }} <= 90 then 4
            when {{ recency_days }} <= 180 then 3
            when {{ recency_days }} <= 365 then 2
            else 1
        end
    {% endset %}
    
    {% set frequency_score %}
        case 
            when {{ frequency_orders }} >= 20 then 5
            when {{ frequency_orders }} >= 10 then 4
            when {{ frequency_orders }} >= 5 then 3
            when {{ frequency_orders }} >= 2 then 2
            else 1
        end
    {% endset %}
    
    {% set monetary_score %}
        case 
            when {{ monetary_value }} >= 1000 then 5
            when {{ monetary_value }} >= 500 then 4
            when {{ monetary_value }} >= 200 then 3
            when {{ monetary_value }} >= 100 then 2
            else 1
        end
    {% endset %}
    
    ({{ recency_score }} + {{ frequency_score }} + {{ monetary_score }}) / 3.0
{% endmacro %}

select 
    customer_id,
    
    -- Composed customer scoring
    {{ customer_score('days_since_last_order', 'total_orders', 'total_spent') }} as rfm_score,
    
    -- Score categorization using composition
    case 
        when {{ customer_score('days_since_last_order', 'total_orders', 'total_spent') }} >= 4.0 then 'Champions'
        when {{ customer_score('days_since_last_order', 'total_orders', 'total_spent') }} >= 3.0 then 'Loyal Customers'
        when {{ customer_score('days_since_last_order', 'total_orders', 'total_spent') }} >= 2.0 then 'Potential Loyalists'
        else 'New Customers'
    end as customer_segment

from {{ ref('customer_metrics') }}