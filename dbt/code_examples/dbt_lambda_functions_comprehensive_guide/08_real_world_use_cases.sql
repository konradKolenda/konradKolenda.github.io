-- 08_real_world_use_cases.sql
-- Real-world applications of functional programming patterns in dbt

-- Use Case 1: Dynamic Data Quality Framework
-- Configuration-driven quality checks using functional patterns

{% set data_quality_config = {
    'completeness_checks': {
        'required_fields': ['customer_id', 'order_id', 'order_date', 'amount'],
        'threshold': 0.95
    },
    'validity_checks': {
        'email_format': {
            'columns': ['customer_email', 'billing_email'],
            'pattern': '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'
        },
        'date_ranges': {
            'order_date': {'min': '2020-01-01', 'max': 'current_date + interval \'1 day\''},
            'birth_date': {'min': 'current_date - interval \'120 years\'', 'max': 'current_date - interval \'13 years\''}
        },
        'numeric_ranges': {
            'amount': {'min': 0, 'max': 100000},
            'quantity': {'min': 1, 'max': 1000}
        }
    },
    'consistency_checks': {
        'referential_integrity': [
            {'child_table': 'orders', 'child_column': 'customer_id', 'parent_table': 'customers', 'parent_column': 'id'},
            {'child_table': 'order_items', 'child_column': 'order_id', 'parent_table': 'orders', 'parent_column': 'id'}
        ]
    }
} %}

-- Functional quality check generators
{% macro generate_completeness_checks(table_name, required_fields, threshold) %}
    {% for field in required_fields %}
        select 
            '{{ table_name }}' as table_name,
            '{{ field }}' as column_name,
            'completeness' as check_type,
            count(*) as total_records,
            count({{ field }}) as non_null_records,
            round(count({{ field }}) * 100.0 / count(*), 2) as completeness_percentage,
            case when count({{ field }}) * 1.0 / count(*) >= {{ threshold }} then 'PASS' else 'FAIL' end as result
        from {{ table_name }}
        {% if not loop.last %} union all {% endif %}
    {% endfor %}
{% endmacro %}

{% macro generate_validity_checks(table_name, validity_config) %}
    {% for check_type, check_details in validity_config.items() %}
        {% if check_type == 'email_format' %}
            {% for column in check_details.columns %}
                select 
                    '{{ table_name }}' as table_name,
                    '{{ column }}' as column_name,
                    'email_validity' as check_type,
                    count(*) as total_records,
                    count(case when {{ column }} ~ '{{ check_details.pattern }}' then 1 end) as valid_records,
                    case when count(case when {{ column }} ~ '{{ check_details.pattern }}' then 1 end) = count({{ column }}) 
                         then 'PASS' else 'FAIL' end as result
                from {{ table_name }}
                where {{ column }} is not null
                {% if not loop.last %} union all {% endif %}
            {% endfor %}
        {% elif check_type == 'date_ranges' %}
            {% for column, range_config in check_details.items() %}
                select 
                    '{{ table_name }}' as table_name,
                    '{{ column }}' as column_name,
                    'date_range_validity' as check_type,
                    count(*) as total_records,
                    count(case when {{ column }} between {{ range_config.min }} and {{ range_config.max }} then 1 end) as valid_records,
                    case when count(case when {{ column }} between {{ range_config.min }} and {{ range_config.max }} then 1 end) = count({{ column }})
                         then 'PASS' else 'FAIL' end as result
                from {{ table_name }}
                where {{ column }} is not null
                {% if not loop.last %} union all {% endif %}
            {% endfor %}
        {% endif %}
    {% endfor %}
{% endmacro %}

-- Use Case 2: ETL Pipeline Orchestration
-- Functional approach to defining and executing ETL stages

{% set etl_pipeline_config = [
    {
        'stage': 'extract',
        'description': 'Extract raw data from sources',
        'dependencies': [],
        'parallel_safe': true,
        'timeout_minutes': 30
    },
    {
        'stage': 'validate_raw',
        'description': 'Validate extracted raw data',
        'dependencies': ['extract'],
        'parallel_safe': true,
        'timeout_minutes': 15
    },
    {
        'stage': 'clean_transform',
        'description': 'Clean and transform data',
        'dependencies': ['validate_raw'],
        'parallel_safe': false,
        'timeout_minutes': 60
    },
    {
        'stage': 'aggregate',
        'description': 'Create aggregated views',
        'dependencies': ['clean_transform'],
        'parallel_safe': true,
        'timeout_minutes': 45
    },
    {
        'stage': 'validate_final',
        'description': 'Final data quality validation',
        'dependencies': ['aggregate'],
        'parallel_safe': true,
        'timeout_minutes': 20
    }
] %}

{% macro execute_stage(stage_name, stage_config) %}
    -- Stage: {{ stage_name }}
    -- Description: {{ stage_config.description }}
    -- Dependencies: {{ stage_config.dependencies | join(', ') }}
    
    {% if stage_name == 'extract' %}
        {{ extract_raw_data() }}
    {% elif stage_name == 'validate_raw' %}
        {{ validate_raw_data() }}
    {% elif stage_name == 'clean_transform' %}
        {{ clean_and_transform_data() }}
    {% elif stage_name == 'aggregate' %}
        {{ create_aggregations() }}
    {% elif stage_name == 'validate_final' %}
        {{ final_validation() }}
    {% endif %}
{% endmacro %}

-- Use Case 3: Dynamic Metric Calculation Framework
-- Functional approach to metric definitions and calculations

{% set metric_definitions = {
    'customer_metrics': {
        'total_customers': {
            'sql': 'count(distinct customer_id)',
            'description': 'Total unique customers'
        },
        'avg_order_value': {
            'sql': 'avg(order_amount)',
            'description': 'Average order value'
        },
        'customer_lifetime_value': {
            'sql': 'sum(order_amount) / count(distinct customer_id)',
            'description': 'Average customer lifetime value'
        }
    },
    'product_metrics': {
        'total_products_sold': {
            'sql': 'sum(quantity)',
            'description': 'Total products sold'
        },
        'avg_product_rating': {
            'sql': 'avg(rating)',
            'description': 'Average product rating'
        },
        'top_selling_products': {
            'sql': 'count(*)',
            'group_by': ['product_name'],
            'order_by': ['count(*) desc'],
            'limit': 10,
            'description': 'Top 10 selling products'
        }
    }
} %}

{% macro generate_metric_query(metric_category, metric_name, metric_config, base_table) %}
    -- Metric: {{ metric_category }}.{{ metric_name }}
    -- Description: {{ metric_config.description }}
    
    select 
        '{{ metric_category }}' as metric_category,
        '{{ metric_name }}' as metric_name,
        '{{ metric_config.description }}' as description,
        {% if metric_config.get('group_by') %}
            {{ metric_config.group_by | join(', ') }},
        {% endif %}
        {{ metric_config.sql }} as metric_value
    from {{ base_table }}
    {% if metric_config.get('group_by') %}
        group by {{ metric_config.group_by | join(', ') }}
    {% endif %}
    {% if metric_config.get('order_by') %}
        order by {{ metric_config.order_by | join(', ') }}
    {% endif %}
    {% if metric_config.get('limit') %}
        limit {{ metric_config.limit }}
    {% endif %}
{% endmacro %}

-- Use Case 4: A/B Test Analysis Framework
-- Functional approach to A/B test statistical analysis

{% set ab_test_config = {
    'test_id': 'homepage_redesign_2024',
    'variants': ['control', 'treatment'],
    'metrics': {
        'conversion_rate': {
            'numerator': 'count(case when converted = true then 1 end)',
            'denominator': 'count(*)',
            'format': 'percentage'
        },
        'avg_session_duration': {
            'aggregation': 'avg(session_duration_seconds)',
            'format': 'duration'
        },
        'revenue_per_user': {
            'aggregation': 'sum(revenue) / count(distinct user_id)',
            'format': 'currency'
        }
    },
    'significance_level': 0.05
} %}

{% macro calculate_ab_test_metrics(test_config, results_table) %}
    with variant_metrics as (
        {% for variant in test_config.variants %}
            select 
                '{{ variant }}' as variant,
                count(*) as sample_size,
                {% for metric_name, metric_config in test_config.metrics.items() %}
                    {% if metric_config.get('numerator') and metric_config.get('denominator') %}
                        {{ metric_config.numerator }} as {{ metric_name }}_numerator,
                        {{ metric_config.denominator }} as {{ metric_name }}_denominator,
                        ({{ metric_config.numerator }}) * 1.0 / ({{ metric_config.denominator }}) as {{ metric_name }}
                    {% else %}
                        {{ metric_config.aggregation }} as {{ metric_name }}
                    {% endif %}
                    {%- if not loop.last -%}, {% endif %}
                {% endfor %}
            from {{ results_table }}
            where variant = '{{ variant }}'
            {% if not loop.last %} union all {% endif %}
        {% endfor %}
    ),
    
    statistical_tests as (
        select 
            c.variant as control_variant,
            t.variant as treatment_variant,
            {% for metric_name, metric_config in test_config.metrics.items() %}
                c.{{ metric_name }} as control_{{ metric_name }},
                t.{{ metric_name }} as treatment_{{ metric_name }},
                abs(t.{{ metric_name }} - c.{{ metric_name }}) / c.{{ metric_name }} as {{ metric_name }}_lift,
                -- Simplified significance test (replace with proper statistical test)
                case 
                    when abs(t.{{ metric_name }} - c.{{ metric_name }}) / c.{{ metric_name }} > 0.05 then 'Significant'
                    else 'Not Significant'
                end as {{ metric_name }}_significance
                {%- if not loop.last -%}, {% endif %}
            {% endfor %}
        from variant_metrics c
        cross join variant_metrics t
        where c.variant = 'control' and t.variant = 'treatment'
    )
    
    select * from statistical_tests
{% endmacro %}

-- Use Case 5: Customer Segmentation Engine
-- Functional approach to dynamic customer segmentation

{% set segmentation_rules = {
    'rfm_segmentation': {
        'recency_buckets': [30, 90, 180, 365],
        'frequency_buckets': [1, 3, 6, 10],
        'monetary_buckets': [100, 300, 800, 2000],
        'segment_definitions': {
            'Champions': {'recency': [4, 5], 'frequency': [4, 5], 'monetary': [4, 5]},
            'Loyal Customers': {'recency': [2, 5], 'frequency': [3, 5], 'monetary': [3, 5]},
            'Potential Loyalists': {'recency': [3, 5], 'frequency': [1, 3], 'monetary': [1, 3]},
            'New Customers': {'recency': [4, 5], 'frequency': [1, 1], 'monetary': [1, 1]},
            'Promising': {'recency': [3, 4], 'frequency': [1, 1], 'monetary': [2, 3]},
            'Need Attention': {'recency': [2, 3], 'frequency': [2, 3], 'monetary': [2, 3]},
            'About to Sleep': {'recency': [2, 3], 'frequency': [1, 2], 'monetary': [1, 2]},
            'At Risk': {'recency': [1, 2], 'frequency': [2, 5], 'monetary': [2, 5]},
            'Cannot Lose Them': {'recency': [1, 2], 'frequency': [4, 5], 'monetary': [4, 5]},
            'Hibernating': {'recency': [1, 2], 'frequency': [1, 2], 'monetary': [1, 2]}
        }
    }
} %}

{% macro generate_rfm_scores(customer_table, rules) %}
    with customer_rfm_raw as (
        select 
            customer_id,
            current_date - max(order_date) as recency_days,
            count(distinct order_id) as frequency_orders,
            sum(order_amount) as monetary_value
        from {{ customer_table }}
        group by customer_id
    ),
    
    customer_rfm_scores as (
        select 
            customer_id,
            recency_days,
            frequency_orders,
            monetary_value,
            
            -- Generate recency score
            {% set r_buckets = rules.rfm_segmentation.recency_buckets %}
            case 
                {% for i in range(r_buckets | length) %}
                    {% if i == 0 %}
                        when recency_days <= {{ r_buckets[i] }} then {{ r_buckets | length - i }}
                    {% else %}
                        when recency_days <= {{ r_buckets[i] }} then {{ r_buckets | length - i }}
                    {% endif %}
                {% endfor %}
                else 1
            end as recency_score,
            
            -- Generate frequency score
            {% set f_buckets = rules.rfm_segmentation.frequency_buckets %}
            case 
                {% for i in range(f_buckets | length) %}
                    {% if i == f_buckets | length - 1 %}
                        when frequency_orders >= {{ f_buckets[i] }} then {{ i + 1 }}
                    {% else %}
                        when frequency_orders >= {{ f_buckets[i] }} and frequency_orders < {{ f_buckets[i + 1] }} then {{ i + 1 }}
                    {% endif %}
                {% endfor %}
                else 1
            end as frequency_score,
            
            -- Generate monetary score
            {% set m_buckets = rules.rfm_segmentation.monetary_buckets %}
            case 
                {% for i in range(m_buckets | length) %}
                    {% if i == m_buckets | length - 1 %}
                        when monetary_value >= {{ m_buckets[i] }} then {{ i + 1 }}
                    {% else %}
                        when monetary_value >= {{ m_buckets[i] }} and monetary_value < {{ m_buckets[i + 1] }} then {{ i + 1 }}
                    {% endif %}
                {% endfor %}
                else 1
            end as monetary_score
        from customer_rfm_raw
    )
    
    select 
        customer_id,
        recency_days,
        frequency_orders,
        monetary_value,
        recency_score,
        frequency_score,
        monetary_score,
        
        -- Assign segments based on RFM scores
        case 
            {% for segment_name, segment_rules in rules.rfm_segmentation.segment_definitions.items() %}
                when recency_score between {{ segment_rules.recency[0] }} and {{ segment_rules.recency[1] }}
                    and frequency_score between {{ segment_rules.frequency[0] }} and {{ segment_rules.frequency[1] }}
                    and monetary_score between {{ segment_rules.monetary[0] }} and {{ segment_rules.monetary[1] }}
                then '{{ segment_name }}'
            {% endfor %}
            else 'Other'
        end as customer_segment
        
    from customer_rfm_scores
{% endmacro %}

-- Usage: Apply the RFM segmentation
{{ generate_rfm_scores(ref('customer_orders'), segmentation_rules) }}