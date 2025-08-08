-- Warehouse-Specific Optimization Examples
-- Platform-specific performance optimizations for Snowflake, BigQuery, and Redshift

-- ============================================================================
-- SNOWFLAKE OPTIMIZATIONS
-- ============================================================================

-- Example 1: Clustering Keys with Performance Monitoring
{{ config(
    materialized='table',
    cluster_by=['order_date', 'customer_segment'],
    automatic_clustering=true,
    alias='orders_snowflake_clustered',
    
    pre_hook=[
        -- Analyze clustering before creation
        "select system$clustering_information('{{ source('raw', 'orders') }}', '(order_date, customer_segment)')"
    ],
    
    post_hook=[
        -- Update table statistics
        "analyze table {{ this }} compute statistics for all columns",
        
        -- Check clustering effectiveness
        "select system$clustering_information('{{ this }}', '(order_date, customer_segment)') as clustering_info",
        
        -- Enable automatic clustering if not already enabled
        "alter table {{ this }} resume recluster"
    ]
) }}

select 
    order_id,
    customer_id,
    order_date,
    customer_segment,
    revenue,
    product_category,
    
    -- Leverage clustering for window functions
    row_number() over (
        partition by customer_segment 
        order by order_date, revenue desc
    ) as segment_revenue_rank,
    
    -- Efficient date-based calculations
    sum(revenue) over (
        partition by customer_segment
        order by order_date 
        range between interval '30 days' preceding and current row
    ) as segment_30d_revenue

from {{ ref('raw_orders') }}
where order_date >= '2023-01-01'


-- Example 2: Snowflake Lateral Column Aliasing
{{ config(
    materialized='view',
    alias='customer_metrics_lateral'
) }}

select 
    customer_id,
    order_date,
    revenue,
    
    -- Base calculation
    revenue * 0.15 as estimated_profit,
    
    -- Reference previous calculation (Snowflake-specific)
    estimated_profit / revenue as profit_margin,
    
    -- Use in conditional logic
    case 
        when profit_margin > 0.20 then 'high_margin'
        when profit_margin > 0.10 then 'medium_margin'
        else 'low_margin'
    end as profitability_tier,
    
    -- Use in window functions
    avg(profit_margin) over (
        partition by customer_id 
        order by order_date 
        rows between 6 preceding and current row
    ) as avg_profit_margin_7d,
    
    -- Complex nested references
    case when avg_profit_margin_7d > profit_margin * 1.2 then 'declining' else 'stable' end as profit_trend

from {{ ref('fact_orders') }}


-- Example 3: Dynamic Warehouse Scaling
{{ config(
    materialized='table',
    alias='large_aggregation_dynamic_wh',
    
    pre_hook=[
        -- Scale up warehouse for heavy computation
        "alter warehouse {{ target.warehouse }} set warehouse_size = 'LARGE'",
        "alter warehouse {{ target.warehouse }} set max_cluster_count = 3",
        "alter warehouse {{ target.warehouse }} set auto_suspend = 300"
    ],
    
    post_hook=[
        -- Scale back down after completion
        "alter warehouse {{ target.warehouse }} set warehouse_size = 'MEDIUM'",
        "alter warehouse {{ target.warehouse }} set max_cluster_count = 1",
        "alter warehouse {{ target.warehouse }} set auto_suspend = 60"
    ]
) }}

with heavy_computation as (
    select 
        customer_id,
        order_month,
        
        -- CPU-intensive aggregations
        sum(revenue) as monthly_revenue,
        count(distinct order_id) as order_count,
        stddev(revenue) as revenue_volatility,
        
        -- Memory-intensive window functions
        percentile_cont(0.5) within group (order by revenue) as median_order_value,
        percentile_cont(0.95) within group (order by revenue) as p95_order_value,
        
        -- Complex statistical calculations
        corr(revenue, extract(day from order_date)) as day_correlation,
        regr_slope(revenue, extract(dayofweek from order_date)) as weekday_trend
        
    from {{ ref('fact_orders') }}
    where order_date >= '2022-01-01'
    group by customer_id, date_trunc('month', order_date)
)

select * from heavy_computation


-- ============================================================================
-- BIGQUERY OPTIMIZATIONS  
-- ============================================================================

-- Example 4: Partitioning and Clustering for BigQuery
{{ config(
    materialized='table',
    partition_by={
        'field': 'order_date',
        'data_type': 'date',
        'granularity': 'day'
    },
    cluster_by=['customer_segment', 'product_category', 'order_status'],
    require_partition_filter=true,
    alias='orders_bigquery_optimized',
    
    post_hook=[
        -- Set partition expiration for cost control
        "alter table {{ this }} set options(partition_expiration_days=1095)",  -- 3 years
        
        -- Add table description
        "alter table {{ this }} set options(description='Partitioned and clustered orders table for optimal query performance')"
    ]
) }}

select 
    order_id,
    customer_id,
    order_date,
    customer_segment,
    product_category,
    order_status,
    revenue,
    
    -- Optimized for partition pruning
    extract(year from order_date) as order_year,
    extract(month from order_date) as order_month,
    
    -- Clustering-optimized calculations
    count(*) over (partition by customer_segment, product_category) as segment_category_orders

from {{ ref('raw_orders') }}
-- Always include partition filter for cost optimization
where order_date >= date_sub(current_date(), interval 2 years)


-- Example 5: BigQuery Approximation Functions
{{ config(
    materialized='table',
    partition_by={
        'field': 'analysis_date',
        'data_type': 'date'
    },
    alias='customer_analytics_approx'
) }}

select 
    current_date() as analysis_date,
    customer_segment,
    
    -- Standard aggregations
    count(*) as total_customers,
    sum(lifetime_value) as total_ltv,
    avg(lifetime_value) as avg_ltv,
    
    -- Approximate functions for better performance
    approx_count_distinct(customer_id) as approx_unique_customers,
    approx_quantiles(lifetime_value, 100)[offset(50)] as approx_median_ltv,
    approx_quantiles(lifetime_value, 100)[offset(95)] as approx_p95_ltv,
    
    -- HyperLogLog for large-scale cardinality
    hll_count.merge(hll_count.init(customer_id)) as hll_customer_count,
    
    -- Approximate top-k for popular items
    approx_top_count(favorite_product, 10) as top_products,
    approx_top_sum(lifetime_value, customer_id, 100) as top_customers_by_value

from {{ ref('customer_profiles') }}
group by customer_segment


-- Example 6: BigQuery Slots Optimization with Labels
{{ config(
    materialized='table',
    labels={'team': 'analytics', 'priority': 'high', 'cost_center': 'marketing'},
    alias='marketing_analysis_labeled'
) }}

select 
    campaign_id,
    customer_segment,
    
    -- Use SAFE functions to prevent errors that waste slots
    safe_divide(total_revenue, total_cost) as roas,
    safe_divide(conversions, impressions) as conversion_rate,
    
    -- Optimize string operations
    regexp_extract(campaign_name, r'([A-Z]{2,})') as campaign_type,
    
    -- Efficient date calculations
    date_diff(last_purchase_date, first_purchase_date, day) as customer_lifespan_days

from {{ ref('marketing_campaigns') }}
where campaign_start_date >= date_sub(current_date(), interval 1 year)


-- ============================================================================
-- REDSHIFT OPTIMIZATIONS
-- ============================================================================

-- Example 7: Distribution and Sort Keys for Redshift
{{ config(
    materialized='table',
    dist='customer_id',  -- Distribute by most common join key
    sort=['order_date', 'customer_id', 'revenue'],  -- Sort by filter columns
    diststyle='key',
    alias='orders_redshift_optimized'
) }}

select 
    order_id,
    customer_id,  -- Distribution key
    order_date,   -- Primary sort key
    revenue,      -- Secondary sort key
    product_category,
    order_status,
    
    -- Window functions benefit from sort keys
    row_number() over (
        partition by customer_id 
        order by order_date
    ) as customer_order_sequence,
    
    -- Cumulative calculations optimized by sort order
    sum(revenue) over (
        partition by customer_id 
        order by order_date 
        rows unbounded preceding
    ) as cumulative_customer_revenue,
    
    -- Date-based analytics benefit from date sorting
    lag(order_date, 1) over (
        partition by customer_id 
        order by order_date
    ) as previous_order_date

from {{ ref('raw_orders') }}
where order_date >= '2023-01-01'


-- Example 8: Redshift Compression and Encoding
{{ config(
    materialized='table',
    dist='customer_id',
    sort=['order_date'],
    alias='orders_redshift_compressed',
    
    post_hook=[
        -- Analyze compression after load
        "analyze compression {{ this }}",
        
        -- Update table statistics
        "analyze {{ this }}",
        
        -- Vacuum to reclaim space and sort
        "vacuum sort only {{ this }}"
    ]
) }}

select 
    order_id::varchar(50) encode lzo,           -- Text compression
    customer_id::int encode delta32k,           -- Integer compression  
    order_date::date encode delta,              -- Date compression
    revenue::decimal(10,2) encode mostly16,     -- Numeric compression
    product_category::varchar(100) encode text255,  -- Category compression
    order_status::varchar(20) encode bytedict, -- Low cardinality compression
    
    -- Calculated fields with appropriate encoding
    extract(year from order_date)::int encode delta32k as order_year,
    extract(month from order_date)::int encode delta32k as order_month,
    extract(dow from order_date)::int encode mostly8 as day_of_week

from {{ ref('raw_orders') }}


-- Example 9: Cross-Platform Optimization Macro
{% macro optimize_for_warehouse() %}
    {% if target.type == 'snowflake' %}
        {{ config(
            cluster_by=['order_date', 'customer_id'],
            automatic_clustering=true
        ) }}
    {% elif target.type == 'bigquery' %}
        {{ config(
            partition_by={'field': 'order_date', 'data_type': 'date'},
            cluster_by=['customer_id', 'product_category'],
            require_partition_filter=true
        ) }}
    {% elif target.type == 'redshift' %}
        {{ config(
            dist='customer_id',
            sort=['order_date', 'customer_id'],
            diststyle='key'
        ) }}
    {% endif %}
{% endmacro %}

-- Usage example
{{ optimize_for_warehouse() }}

select 
    order_id,
    customer_id,
    order_date,
    revenue,
    
    -- Warehouse-agnostic window function
    {% if target.type == 'snowflake' %}
        -- Use Snowflake-specific optimizations
        row_number() over (partition by customer_id order by order_date) 
    {% else %}
        -- Standard SQL for other warehouses
        row_number() over (partition by customer_id order by order_date)
    {% endif %} as order_sequence

from {{ ref('raw_orders') }}