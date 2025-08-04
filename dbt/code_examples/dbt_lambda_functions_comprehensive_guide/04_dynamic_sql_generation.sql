-- 04_dynamic_sql_generation.sql
-- Dynamic SQL generation using lambda-like patterns

-- Generate pivot columns based on actual data
{% macro generate_pivot_columns(table_name, pivot_column, value_column, aggregation='sum') %}
    {% set pivot_query %}
        select distinct {{ pivot_column }} 
        from {{ table_name }} 
        where {{ pivot_column }} is not null
        order by {{ pivot_column }}
    {% endset %}
    
    {% if execute %}
        {% set results = run_query(pivot_query) %}
        {% set pivot_values = results.columns[0].values() %}
    {% else %}
        {% set pivot_values = [] %}
    {% endif %}
    
    {% for value in pivot_values %}
        {{ aggregation }}(case when {{ pivot_column }} = '{{ value }}' 
            then {{ value_column }} else 0 end) as {{ value | replace(' ', '_') | replace('-', '_') | lower }}
        {%- if not loop.last -%}, {% endif %}
    {% endfor %}
{% endmacro %}

-- Conditional column selection based on variables
{% macro conditional_select(table_name, column_configs) %}
    select 
        {% for config in column_configs %}
            {% if config.condition %}
                {{ config.expression }} as {{ config.alias }}
                {%- if not loop.last -%}, {% endif %}
            {% endif %}
        {% endfor %}
    from {{ table_name }}
{% endmacro %}

-- Generate date spine with lambda-like date functions
{% macro generate_date_series(start_date, end_date, interval_type='day', date_format='YYYY-MM-DD') %}
    {% set date_sql %}
        {% if interval_type == 'day' %}
            generate_series('{{ start_date }}'::date, '{{ end_date }}'::date, '1 day'::interval)
        {% elif interval_type == 'week' %}
            generate_series('{{ start_date }}'::date, '{{ end_date }}'::date, '1 week'::interval)
        {% elif interval_type == 'month' %}
            generate_series('{{ start_date }}'::date, '{{ end_date }}'::date, '1 month'::interval)
        {% endif %}
    {% endset %}
    
    with date_spine as (
        select {{ date_sql }} as date_value
    )
    select 
        date_value,
        to_char(date_value, '{{ date_format }}') as formatted_date,
        extract(year from date_value) as year,
        extract(month from date_value) as month,
        extract(day from date_value) as day,
        extract(dow from date_value) as day_of_week
    from date_spine
{% endmacro %}

-- Dynamic WHERE clause builder
{% macro build_where_clause(conditions) %}
    {% if conditions %}
        where 
        {% for condition in conditions %}
            {{ condition.column }} {{ condition.operator }} {{ condition.value }}
            {%- if not loop.last %} {{ condition.get('logic', 'and') }} {% endif %}
        {% endfor %}
    {% endif %}
{% endmacro %}

-- Generate aggregation queries based on configuration
{% macro generate_aggregations(base_table, group_by_columns, metrics_config) %}
    select 
        {{ group_by_columns | join(', ') }},
        {% for metric_name, config in metrics_config.items() %}
            {{ config.aggregation }}({{ config.column }}) as {{ metric_name }}
            {%- if not loop.last -%}, {% endif %}
        {% endfor %}
    from {{ base_table }}
    group by {{ range(1, group_by_columns | length + 1) | join(', ') }}
{% endmacro %}

-- Usage examples

-- Example 1: Dynamic pivot for product categories
{{ config(materialized='table') }}

select 
    customer_id,
    order_month,
    {{ generate_pivot_columns(ref('order_details'), 'product_category', 'amount', 'sum') }}
from {{ ref('order_details') }}
group by customer_id, order_month

-- Example 2: Conditional column selection based on feature flags
{% set feature_flags = {
    'include_advanced_metrics': var('advanced_metrics', false),
    'include_experimental_features': var('experimental_features', false),
    'include_deprecated_columns': var('deprecated_columns', false)
} %}

{% set column_configs = [
    {'condition': true, 'expression': 'customer_id', 'alias': 'customer_id'},
    {'condition': true, 'expression': 'revenue', 'alias': 'total_revenue'},
    {'condition': feature_flags.include_advanced_metrics, 'expression': 'customer_lifetime_value', 'alias': 'clv'},
    {'condition': feature_flags.include_experimental_features, 'expression': 'predictive_score', 'alias': 'prediction'},
    {'condition': feature_flags.include_deprecated_columns, 'expression': 'legacy_field', 'alias': 'legacy_data'}
] %}

{{ conditional_select(ref('customer_base'), column_configs) }}

-- Example 3: Dynamic aggregation based on configuration
{% set aggregation_config = {
    'total_revenue': {'aggregation': 'sum', 'column': 'revenue'},
    'avg_order_value': {'aggregation': 'avg', 'column': 'order_value'},
    'order_count': {'aggregation': 'count', 'column': '*'},
    'max_order_date': {'aggregation': 'max', 'column': 'order_date'}
} %}

{{ generate_aggregations(ref('orders'), ['customer_id', 'order_year'], aggregation_config) }}