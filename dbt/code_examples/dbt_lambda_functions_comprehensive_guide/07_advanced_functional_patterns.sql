-- 07_advanced_functional_patterns.sql
-- Advanced functional programming patterns: currying, memoization, and closures

-- Currying: Transform multi-parameter functions into single-parameter functions
{% macro curry_comparison(operator) %}
    {% macro compare(value) %}
        {% macro with_threshold(threshold) %}
            {{ value }} {{ operator }} {{ threshold }}
        {% endmacro %}
        {{ with_threshold }}
    {% endmacro %}
    {{ compare }}
{% endmacro %}

-- Specialized comparison functions using currying
{% macro create_greater_than(threshold) %}
    {% macro gt_check(column_name) %}
        {{ column_name }} > {{ threshold }}
    {% endmacro %}
    {{ gt_check }}
{% endmacro %}

{% macro create_between_check(min_val, max_val) %}
    {% macro between_check(column_name) %}
        {{ column_name }} between {{ min_val }} and {{ max_val }}
    {% endmacro %}
    {{ between_check }}
{% endmacro %}

-- Partial application pattern
{% macro create_date_filter(comparison_op, reference_date) %}
    {% macro date_condition(date_column) %}
        {{ date_column }} {{ comparison_op }} '{{ reference_date }}'
    {% endmacro %}
    {{ date_condition }}
{% endmacro %}

-- Memoization pattern using Jinja variables
{% set _query_cache = {} %}

{% macro memoized_query(cache_key, sql_query) %}
    {% if cache_key in _query_cache %}
        {{ log("Cache hit for key: " ~ cache_key, info=false) }}
        {{ _query_cache[cache_key] }}
    {% else %}
        {% if execute %}
            {% set result = run_query(sql_query) %}
            {% set _ = _query_cache.update({cache_key: result}) %}
            {{ log("Cache miss for key: " ~ cache_key, info=false) }}
            {{ result }}
        {% else %}
            {% set _ = _query_cache.update({cache_key: []}) %}
            {{ [] }}
        {% endif %}
    {% endif %}
{% endmacro %}

-- Closure pattern: Functions that capture their environment
{% macro create_metric_calculator(base_table, date_column) %}
    {% macro calculate_metric(metric_name, aggregation, filter_condition=none) %}
        {% set sql %}
            select 
                {{ aggregation }}({{ metric_name }}) as {{ metric_name }}_{{ aggregation }}
            from {{ base_table }}
            {% if filter_condition %}
                where {{ filter_condition }}
            {% endif %}
        {% endset %}
        {{ memoized_query(metric_name ~ '_' ~ aggregation ~ '_' ~ (filter_condition or 'no_filter'), sql) }}
    {% endmacro %}
    {{ calculate_metric }}
{% endmacro %}

-- Functional factory pattern
{% macro create_validator_factory(validation_rules) %}
    {% macro validate_record(record_columns) %}
        {% set validation_conditions = [] %}
        {% for rule in validation_rules %}
            {% if rule.type == 'not_null' %}
                {% set condition = record_columns[rule.column] ~ ' is not null' %}
            {% elif rule.type == 'range' %}
                {% set condition = record_columns[rule.column] ~ ' between ' ~ rule.min ~ ' and ' ~ rule.max %}
            {% elif rule.type == 'regex' %}
                {% set condition = record_columns[rule.column] ~ ' ~ \'' ~ rule.pattern ~ '\'' %}
            {% elif rule.type == 'custom' %}
                {% set condition = rule.condition.replace('COLUMN', record_columns[rule.column]) %}
            {% endif %}
            {% set _ = validation_conditions.append(condition) %}
        {% endfor %}
        case when {{ validation_conditions | join(' and ') }} then true else false end
    {% endmacro %}
    {{ validate_record }}
{% endmacro %}

-- Monadic pattern for error handling
{% macro maybe_divide(numerator, denominator) %}
    {% macro bind(next_function) %}
        case 
            when {{ denominator }} = 0 or {{ denominator }} is null then null
            else {{ next_function(numerator ~ '/' ~ denominator) }}
        end
    {% endmacro %}
    {{ bind }}
{% endmacro %}

-- Lens pattern for nested data access
{% macro create_json_lens(json_column, path) %}
    {% macro get() %}
        {{ json_column }}::json->'{{ path | join("'->\"") }}'
    {% endmacro %}
    
    {% macro set(new_value) %}
        jsonb_set({{ json_column }}::jsonb, '{{{ path | join(",") }}}', '{{ new_value }}'::jsonb)
    {% endmacro %}
    
    {% macro update(transform_func) %}
        jsonb_set({{ json_column }}::jsonb, '{{{ path | join(",") }}}', 
                  {{ transform_func(json_column ~ "::json->'" ~ path | join("'->\"") ~ "'") }}::jsonb)
    {% endmacro %}
    
    {{ {'get': get, 'set': set, 'update': update} }}
{% endmacro %}

-- Usage Examples

-- Example 1: Curried validation functions
{% set revenue_validator = create_greater_than(1000) %}
{% set score_validator = create_between_check(0, 100) %}
{% set recent_date_filter = create_date_filter('>=', '2023-01-01') %}

select 
    customer_id,
    revenue,
    customer_score,
    last_order_date,
    
    -- Using curried functions
    {{ revenue_validator('revenue') }} as is_high_value,
    {{ score_validator('customer_score') }} as valid_score,
    {{ recent_date_filter('last_order_date') }} as is_recent_customer

from {{ ref('customers') }}

-- Example 2: Memoized expensive calculations
{% set top_customers_query = "select customer_id from customers order by revenue desc limit 100" %}
{% set customer_segments_query = "select distinct segment from customer_segments" %}

-- These queries will be cached on subsequent calls
{% set top_customers = memoized_query('top_100_customers', top_customers_query) %}
{% set segments = memoized_query('customer_segments', customer_segments_query) %}

select 
    customer_id,
    revenue,
    case 
        when customer_id in ({{ top_customers.columns[0].values() | join(',') }}) then 'Top Customer'
        else 'Regular Customer'
    end as customer_tier

from {{ ref('customers') }}

-- Example 3: Factory pattern for validation
{% set customer_validation_rules = [
    {'type': 'not_null', 'column': 'customer_id'},
    {'type': 'regex', 'column': 'email', 'pattern': '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'},
    {'type': 'range', 'column': 'age', 'min': 18, 'max': 120},
    {'type': 'custom', 'column': 'phone', 'condition': 'length(regexp_replace(COLUMN, \'[^0-9]\', \'\', \'g\')) >= 10'}
] %}

{% set customer_validator = create_validator_factory(customer_validation_rules) %}

select 
    customer_id,
    email,
    age,
    phone,
    
    -- Apply factory-generated validator
    {{ customer_validator({
        'customer_id': 'customer_id',
        'email': 'email', 
        'age': 'age',
        'phone': 'phone'
    }) }} as is_valid_record

from {{ ref('raw_customers') }}

-- Example 4: Functional composition with error handling
{% macro safe_calculation_chain(value, operations) %}
    {% set result = value %}
    {% for op in operations %}
        {% if op.type == 'divide' %}
            {% set result %}
                case 
                    when ({{ result }}) is null or {{ op.divisor }} = 0 then null
                    else ({{ result }}) / {{ op.divisor }}
                end
            {% endset %}
        {% elif op.type == 'multiply' %}
            {% set result %}
                case 
                    when ({{ result }}) is null then null
                    else ({{ result }}) * {{ op.factor }}
                end
            {% endset %}
        {% elif op.type == 'log' %}
            {% set result %}
                case 
                    when ({{ result }}) is null or ({{ result }}) <= 0 then null
                    else ln({{ result }})
                end
            {% endset %}
        {% endif %}
    {% endfor %}
    {{ result }}
{% endmacro %}

-- Using safe calculation chain
{% set price_calculation_steps = [
    {'type': 'multiply', 'factor': 1.2},  -- Add 20% markup
    {'type': 'divide', 'divisor': 'quantity'},  -- Get unit price
    {'type': 'log'}  -- Log transform for analysis
] %}

select 
    order_id,
    base_price,
    quantity,
    
    {{ safe_calculation_chain('base_price', price_calculation_steps) }} as transformed_unit_price

from {{ ref('orders') }}

-- Example 5: Functional pipeline with conditional execution
{% macro conditional_pipeline(input_value, pipeline_config) %}
    {% set result = input_value %}
    {% for step in pipeline_config %}
        {% if step.get('condition', 'true') %}
            {% if step.type == 'transform' %}
                {% set result = step.function(result, **step.get('params', {})) %}
            {% elif step.type == 'validate' %}
                {% set result %}
                    case 
                        when {{ step.validation(result) }} then {{ result }}
                        else {{ step.get('default', 'null') }}
                    end
                {% endset %}
            {% endif %}
        {% endif %}
    {% endfor %}
    {{ result }}
{% endmacro %}