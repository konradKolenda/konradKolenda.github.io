-- 05_functional_control_structures.sql
-- Functional programming control structures using Jinja

-- Map pattern: Apply function to each element in a collection
{% macro map_transform(items, transform_func, transform_params={}) %}
    {% set results = [] %}
    {% for item in items %}
        {% set transformed = transform_func(item, **transform_params) %}
        {% set _ = results.append(transformed) %}
    {% endfor %}
    {{ results }}
{% endmacro %}

-- Filter pattern: Select items based on predicate function
{% macro filter_items(items, predicate_func, predicate_params={}) %}
    {% set filtered = [] %}
    {% for item in items %}
        {% if predicate_func(item, **predicate_params) %}
            {% set _ = filtered.append(item) %}
        {% endif %}
    {% endfor %}
    {{ filtered }}
{% endmacro %}

-- Reduce pattern: Aggregate items using operation
{% macro reduce_items(items, operation, initial_value=0) %}
    {% set result = initial_value %}
    {% for item in items %}
        {% if operation == 'sum' %}
            {% set result = result + item %}
        {% elif operation == 'multiply' %}
            {% set result = result * item %}
        {% elif operation == 'concat' %}
            {% set result = result ~ item %}
        {% elif operation == 'max' %}
            {% set result = [result, item] | max %}
        {% elif operation == 'min' %}
            {% set result = [result, item] | min %}
        {% endif %}
    {% endfor %}
    {{ result }}
{% endmacro %}

-- Functional iteration over columns with conditions
{% macro iterate_columns_conditional(columns, condition_func, transform_func) %}
    {% for column in columns %}
        {% if condition_func(column) %}
            {{ transform_func(column) }}
            {%- if not loop.last %}, {% endif %}
        {% endif %}
    {% endfor %}
{% endmacro %}

-- Recursive macro for nested data structures
{% macro process_nested_config(config, level=0) %}
    {% for key, value in config.items() %}
        {% if value is mapping %}
            -- Nested configuration found at level {{ level }}
            {{ process_nested_config(value, level + 1) }}
        {% else %}
            {{ key }}: {{ value }}
        {% endif %}
    {% endfor %}
{% endmacro %}

-- Functional pipeline with error handling
{% macro functional_pipeline(data, pipeline_steps) %}
    {% set current_data = data %}
    {% for step in pipeline_steps %}
        {% set step_name = step.name %}
        {% set step_func = step.function %}
        {% set step_params = step.get('params', {}) %}
        
        {{ log("Processing step: " ~ step_name, info=true) }}
        
        {% if step.get('condition', true) %}
            {% set current_data = step_func(current_data, **step_params) %}
        {% else %}
            {{ log("Skipping step: " ~ step_name, info=true) }}
        {% endif %}
    {% endfor %}
    {{ current_data }}
{% endmacro %}

-- Usage examples in a model context

-- Example 1: Functional column processing
{% set numeric_columns = ['revenue', 'cost', 'profit', 'quantity', 'price'] %}
{% set text_columns = ['customer_name', 'product_name', 'category', 'status'] %}

-- Function to check if column should be normalized
{% macro should_normalize(column_name) %}
    {{ column_name in ['revenue', 'cost', 'profit'] }}
{% endmacro %}

-- Function to normalize column
{% macro normalize_column(column_name) %}
    ({{ column_name }} - min({{ column_name }}) over()) / 
    (max({{ column_name }}) over() - min({{ column_name }}) over()) as {{ column_name }}_normalized
{% endmacro %}

-- Function to clean text column
{% macro clean_text_column(column_name) %}
    upper(trim({{ column_name }})) as {{ column_name }}_clean
{% endmacro %}

select 
    order_id,
    customer_id,
    
    -- Apply normalization conditionally to numeric columns
    {{ iterate_columns_conditional(numeric_columns, should_normalize, normalize_column) }},
    
    -- Apply text cleaning to all text columns
    {% for col in text_columns %}
        {{ clean_text_column(col) }}
        {%- if not loop.last %}, {% endif %}
    {% endfor %}

from {{ ref('raw_orders') }}

-- Example 2: Functional aggregation with grouping
{% macro create_aggregation_select(metrics, group_by_cols) %}
    select 
        {{ group_by_cols | join(', ') }},
        {% for metric_name, metric_config in metrics.items() %}
            {{ metric_config.func }}({{ metric_config.column }}) as {{ metric_name }}
            {%- if not loop.last %}, {% endif %}
        {% endfor %}
    from {{ ref('base_data') }}
    group by {{ range(1, group_by_cols|length + 1) | join(', ') }}
{% endmacro %}

-- Example 3: Conditional feature generation
{% set feature_configs = [
    {
        'name': 'customer_segment',
        'condition': var('enable_segmentation', true),
        'function': 'case when revenue > 1000 then "high_value" else "standard" end'
    },
    {
        'name': 'risk_score',
        'condition': var('enable_risk_scoring', false),
        'function': 'case when days_since_last_order > 90 then "high_risk" else "low_risk" end'
    },
    {
        'name': 'lifetime_value_bucket',
        'condition': var('enable_ltv_bucketing', true),
        'function': 'ntile(5) over (order by customer_lifetime_value)'
    }
] %}

with base as (
    select * from {{ ref('customer_base') }}
),

features as (
    select 
        *,
        {% for feature in feature_configs %}
            {% if feature.condition %}
                {{ feature.function }} as {{ feature.name }}
                {%- if not loop.last %}, {% endif %}
            {% endif %}
        {% endfor %}
    from base
)

select * from features

-- Example 4: Functional data quality checks
{% macro create_quality_check(check_name, check_condition, severity='error') %}
    select 
        '{{ check_name }}' as check_name,
        count(*) as failure_count,
        '{{ severity }}' as severity
    from {{ ref('target_table') }}
    where not ({{ check_condition }})
{% endmacro %}

{% set quality_checks = [
    {'name': 'not_null_customer_id', 'condition': 'customer_id is not null'},
    {'name': 'positive_revenue', 'condition': 'revenue >= 0'},
    {'name': 'valid_email_format', 'condition': 'email ~ \'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$\''},
    {'name': 'future_order_date', 'condition': 'order_date <= current_date'}
] %}

-- Generate quality check union
{% for check in quality_checks %}
    {{ create_quality_check(check.name, check.condition) }}
    {% if not loop.last %} union all {% endif %}
{% endfor %}