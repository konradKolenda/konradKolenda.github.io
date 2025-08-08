-- Cost Optimization Strategies for dbt
-- Comprehensive examples for minimizing warehouse costs while maintaining performance

-- ============================================================================
-- DYNAMIC WAREHOUSE SIZING
-- ============================================================================

-- Example 1: Data-Volume Based Warehouse Scaling
{% macro get_estimated_row_count(relation) %}
    {% if execute %}
        {% set row_count_query %}
            select count(*) as row_count from {{ relation }}
        {% endset %}
        {% set results = run_query(row_count_query) %}
        {% if results %}
            {{ return(results.columns[0].values()[0]) }}
        {% endif %}
    {% endif %}
    {{ return(0) }}
{% endmacro %}

{% set estimated_rows = get_estimated_row_count(ref('raw_orders')) %}

{{ config(
    materialized='table',
    alias='orders_dynamic_sizing',
    
    pre_hook=[
        -- Scale warehouse based on data volume
        "{% if estimated_rows > 50000000 %}",
        "alter warehouse {{ target.warehouse }} set warehouse_size = 'X-LARGE';",
        "{% elif estimated_rows > 10000000 %}",
        "alter warehouse {{ target.warehouse }} set warehouse_size = 'LARGE';",
        "{% elif estimated_rows > 1000000 %}",
        "alter warehouse {{ target.warehouse }} set warehouse_size = 'MEDIUM';",
        "{% else %}",
        "alter warehouse {{ target.warehouse }} set warehouse_size = 'SMALL';",
        "{% endif %}",
        
        -- Log scaling decision
        "insert into {{ target.database }}.analytics.warehouse_scaling_log values (
            '{{ this.identifier }}',
            {{ estimated_rows }},
            '{% if estimated_rows > 50000000 %}X-LARGE{% elif estimated_rows > 10000000 %}LARGE{% elif estimated_rows > 1000000 %}MEDIUM{% else %}SMALL{% endif %}',
            current_timestamp()
        );"
    ],
    
    post_hook=[
        -- Scale back to default size after processing
        "alter warehouse {{ target.warehouse }} set warehouse_size = 'MEDIUM';",
        "alter warehouse {{ target.warehouse }} set auto_suspend = 60;"
    ]
) }}

with processed_orders as (
    select 
        order_id,
        customer_id,
        order_date,
        revenue,
        
        -- Complex aggregations that benefit from larger warehouse
        sum(revenue) over (
            partition by customer_id 
            order by order_date 
            rows unbounded preceding
        ) as cumulative_revenue,
        
        -- CPU-intensive calculations
        percentile_cont(0.5) within group (order by revenue) over (
            partition by date_trunc('month', order_date)
        ) as monthly_median_revenue,
        
        -- Data volume metadata
        {{ estimated_rows }} as total_source_rows,
        '{{ config.get('pre_hook')[0] }}' as scaling_strategy
        
    from {{ ref('raw_orders') }}
    where order_date >= '2022-01-01'
)

select * from processed_orders


-- ============================================================================
-- INTELLIGENT DATA SAMPLING FOR DEVELOPMENT
-- ============================================================================

-- Example 2: Environment-Based Data Sampling
{{ config(
    materialized={% if target.name == 'dev' %}'view'{% else %}'table'{% endif %},
    alias='customer_analytics_sampled'
) }}

{% set sample_config = {
    'dev': {'sample_pct': 1, 'time_filter': 30},      -- 1% sample, last 30 days
    'ci': {'sample_pct': 5, 'time_filter': 90},       -- 5% sample, last 90 days  
    'staging': {'sample_pct': 50, 'time_filter': 365}, -- 50% sample, last year
    'prod': {'sample_pct': 100, 'time_filter': 9999}   -- Full data
} %}

{% set env_config = sample_config.get(target.name, sample_config['prod']) %}

with sampled_orders as (
    select *
    from {{ ref('raw_orders') }}
    where order_date >= current_date - {{ env_config['time_filter'] }}
    
    {% if env_config['sample_pct'] < 100 %}
        -- Use deterministic sampling for consistency
        and abs(hash(order_id)) % 100 < {{ env_config['sample_pct'] }}
    {% endif %}
),

customer_metrics as (
    select 
        customer_id,
        count(*) as total_orders,
        sum(revenue) as total_revenue,
        avg(revenue) as avg_order_value,
        max(order_date) as last_order_date,
        min(order_date) as first_order_date,
        
        -- Add sampling metadata
        {{ env_config['sample_pct'] }}::float as sample_percentage,
        count(*) * (100.0 / {{ env_config['sample_pct'] }}) as estimated_full_orders,
        
        {% if target.name == 'dev' %}
            'development_sample' as data_quality_note
        {% else %}
            'production_data' as data_quality_note
        {% endif %}
        
    from sampled_orders
    group by customer_id
)

select * from customer_metrics


-- ============================================================================
-- COST-AWARE QUERY PATTERNS
-- ============================================================================

-- Example 3: Efficient Approximation for Large Datasets
{{ config(
    materialized='table',
    alias='marketing_attribution_cost_optimized'
) }}

with campaign_data as (
    select 
        campaign_id,
        customer_id,
        attribution_date,
        revenue,
        cost
    from {{ ref('marketing_attribution') }}
    where attribution_date >= current_date - 30
),

-- Use approximation functions for large-scale analytics
campaign_performance as (
    select 
        campaign_id,
        
        -- Standard metrics
        count(*) as total_attributions,
        sum(revenue) as total_attributed_revenue,
        sum(cost) as total_cost,
        
        {% if target.type == 'bigquery' %}
            -- BigQuery approximation functions for cost efficiency
            approx_count_distinct(customer_id) as approx_unique_customers,
            approx_quantiles(revenue, 100)[offset(50)] as approx_median_revenue,
            approx_quantiles(revenue, 100)[offset(95)] as approx_p95_revenue,
            
        {% elif target.type == 'snowflake' %}
            -- Snowflake HLL for large cardinality estimation
            hll(customer_id) as customer_cardinality_hll,
            approx_percentile(revenue, 0.50) as approx_median_revenue,
            approx_percentile(revenue, 0.95) as approx_p95_revenue,
            
        {% else %}
            -- Standard aggregations for other warehouses
            count(distinct customer_id) as unique_customers,
            percentile_cont(0.50) within group (order by revenue) as median_revenue,
            percentile_cont(0.95) within group (order by revenue) as p95_revenue,
            
        {% endif %}
        
        -- Efficiency metrics
        sum(revenue) / sum(cost) as roas,
        count(*) / count(distinct customer_id) as avg_attributions_per_customer
        
    from campaign_data
    group by campaign_id
),

-- Cost-efficient ranking using sample data for initial filtering
top_campaigns_sample as (
    select campaign_id
    from campaign_data
    {% if target.type == 'snowflake' %}
        sample (10)  -- Sample 10% for initial ranking
    {% else %}
        -- Alternative sampling for other warehouses
        where abs(hash(campaign_id)) % 10 = 0
    {% endif %}
    group by campaign_id
    having sum(revenue) > 10000
    order by sum(revenue) desc
    limit 50
),

-- Full calculation only for top-performing campaigns
final_results as (
    select cp.*
    from campaign_performance cp
    inner join top_campaigns_sample tcs using (campaign_id)
)

select * from final_results
order by total_attributed_revenue desc


-- ============================================================================
-- AUTOMATED COST MONITORING AND ALERTING
-- ============================================================================

-- Example 4: Cost Tracking with Automated Alerts
{% macro track_daily_costs() %}
    {% if target.type == 'snowflake' %}
        {% set cost_tracking_sql %}
            create table if not exists {{ target.database }}.analytics.daily_cost_tracking (
                tracking_date date,
                warehouse_name varchar(100),
                warehouse_size varchar(50),
                credits_used float,
                cost_usd float,
                query_count int,
                avg_query_duration_seconds float,
                cost_per_query float,
                efficiency_score float
            );
            
            -- Calculate today's costs
            with todays_usage as (
                select 
                    current_date() as tracking_date,
                    warehouse_name,
                    
                    -- Get current warehouse size
                    (select warehouse_size 
                     from table(information_schema.warehouses) 
                     where warehouse_name = wh.warehouse_name) as warehouse_size,
                     
                    sum(credits_used) as credits_used,
                    sum(credits_used) * 2.0 as cost_usd,  -- $2 per credit
                    
                    -- Query efficiency metrics
                    count(*) as query_count,
                    avg(total_elapsed_time / 1000) as avg_query_duration_seconds,
                    (sum(credits_used) * 2.0) / count(*) as cost_per_query,
                    
                    -- Efficiency score (queries per dollar)
                    count(*) / (sum(credits_used) * 2.0) as efficiency_score
                    
                from table(information_schema.warehouse_metering_history(
                    date_range_start => current_date(),
                    date_range_end => dateadd(day, 1, current_date())
                )) wh
                where warehouse_name = '{{ target.warehouse }}'
                group by warehouse_name
            )
            
            insert into {{ target.database }}.analytics.daily_cost_tracking
            select * from todays_usage;
            
            -- Check for cost anomalies
            with cost_analysis as (
                select 
                    tracking_date,
                    warehouse_name,
                    cost_usd,
                    lag(cost_usd, 1) over (order by tracking_date) as yesterday_cost,
                    avg(cost_usd) over (order by tracking_date rows between 7 preceding and 1 preceding) as avg_7d_cost,
                    
                    case 
                        when cost_usd > avg_7d_cost * 2.0 then 'high_cost_alert'
                        when cost_usd > avg_7d_cost * 1.5 then 'cost_warning'
                        else 'normal'
                    end as cost_status
                    
                from {{ target.database }}.analytics.daily_cost_tracking
                where warehouse_name = '{{ target.warehouse }}'
                  and tracking_date >= current_date - 8
                order by tracking_date desc
                limit 1
            )
            
            select * from cost_analysis;
        {% endset %}
        
        {% set results = run_query(cost_tracking_sql) %}
        
        {% if execute and results.rows %}
            {% set cost_status = results.columns[5].values()[0] %}
            {% set daily_cost = results.columns[2].values()[0] %}
            
            {% if cost_status == 'high_cost_alert' %}
                {{ log("🚨 HIGH COST ALERT: Daily spend $" ~ daily_cost ~ " is >2x recent average", info=true) }}
            {% elif cost_status == 'cost_warning' %}
                {{ log("⚠️  COST WARNING: Daily spend $" ~ daily_cost ~ " is 50% above recent average", info=true) }}
            {% else %}
                {{ log("💰 Daily cost tracking: $" ~ daily_cost ~ " (normal range)", info=true) }}
            {% endif %}
        {% endif %}
    {% endif %}
{% endmacro %}


-- ============================================================================
-- INTELLIGENT CACHING STRATEGIES
-- ============================================================================

-- Example 5: Multi-Layer Caching for Cost Efficiency
{{ config(
    materialized='table',
    alias='customer_segments_cached'
) }}

-- Layer 1: Fast lookup cache for recent data
with recent_customer_cache as (
    select 
        customer_id,
        current_segment,
        segment_updated_date,
        total_orders_ytd,
        total_revenue_ytd
    from {{ ref('customer_segments') }}
    where segment_updated_date >= current_date - 7  -- Hot cache: last week
),

-- Layer 2: Medium-term cache for stable segments  
stable_segments_cache as (
    select 
        customer_id,
        current_segment,
        segment_updated_date,
        total_orders_ytd,
        total_revenue_ytd
    from {{ ref('customer_segments') }}  
    where segment_updated_date < current_date - 7
      and segment_updated_date >= current_date - 90  -- Warm cache: 7-90 days
      and current_segment in ('platinum', 'gold')    -- Stable high-value segments
),

-- Layer 3: Compute on-demand for old/changing data
computed_segments as (
    select 
        c.customer_id,
        
        -- Real-time segment calculation for dynamic customers
        case 
            when c.total_revenue_ytd > 10000 then 'platinum'
            when c.total_revenue_ytd > 5000 then 'gold' 
            when c.total_revenue_ytd > 1000 then 'silver'
            else 'bronze'
        end as current_segment,
        
        current_date as segment_updated_date,
        c.total_orders_ytd,
        c.total_revenue_ytd
        
    from {{ ref('customer_base_metrics') }} c
    where c.customer_id not in (
        select customer_id from recent_customer_cache
        union
        select customer_id from stable_segments_cache
    )
),

-- Combine all caching layers
final_segments as (
    select *, 'hot_cache' as data_source from recent_customer_cache
    union all
    select *, 'warm_cache' as data_source from stable_segments_cache  
    union all
    select *, 'computed' as data_source from computed_segments
)

select * from final_segments


-- ============================================================================
-- DEVELOPMENT COST CONTROLS
-- ============================================================================

-- Example 6: Development Environment Cost Safeguards
{% macro enforce_dev_limits() %}
    {% if target.name == 'dev' %}
        {% set dev_limits_check %}
            with current_session as (
                select 
                    count(*) as query_count,
                    sum(total_elapsed_time) / 1000 / 3600 as total_hours,
                    sum(total_elapsed_time) / 1000 / 3600 * 2.0 as estimated_cost
                from table(information_schema.query_history_by_session())
                where start_time >= dateadd(hour, -1, current_timestamp)
            )
            
            select 
                *,
                case 
                    when estimated_cost > 5.0 then 'cost_limit_exceeded'
                    when query_count > 100 then 'query_limit_exceeded'
                    when total_hours > 0.5 then 'time_limit_exceeded'
                    else 'within_limits'
                end as limit_status
            from current_session;
        {% endset %}
        
        {% set results = run_query(dev_limits_check) %}
        
        {% if execute and results.rows %}
            {% set status = results.columns[3].values()[0] %}
            {% set cost = results.columns[2].values()[0] %}
            
            {% if status != 'within_limits' %}
                {{ log("🛑 DEVELOPMENT LIMIT EXCEEDED: " ~ status, info=true) }}
                {{ log("   Estimated cost this hour: $" ~ cost, info=true) }}
                {{ log("   Consider using sampling or smaller datasets", info=true) }}
            {% endif %}
        {% endif %}
    {% endif %}
{% endmacro %}

-- Usage in development models
{{ config(
    materialized={% if target.name == 'dev' %}'view'{% else %}'table'{% endif %},
    pre_hook="{{ enforce_dev_limits() }}",
    alias='dev_safe_analytics'
) }}

select 
    customer_id,
    
    {% if target.name == 'dev' %}
        -- Simplified calculations for development
        count(*) as order_count,
        sum(revenue) as total_revenue,
        'dev_simplified' as calculation_mode
    {% else %}
        -- Full calculations for production
        count(*) as order_count,
        sum(revenue) as total_revenue,
        avg(revenue) as avg_order_value,
        stddev(revenue) as revenue_volatility,
        percentile_cont(0.5) within group (order by revenue) as median_revenue,
        'prod_complete' as calculation_mode
    {% endif %}
    
from {{ ref('raw_orders') }}

{% if target.name == 'dev' %}
    -- Limit data volume in development
    where order_date >= current_date - 30
      and abs(hash(customer_id)) % 10 = 0  -- 10% sample
{% endif %}

group by customer_id


-- ============================================================================
-- COST OPTIMIZATION REPORTING
-- ============================================================================

-- Example 7: Cost Optimization Dashboard
{{ config(
    materialized='table',
    alias='cost_optimization_report'
) }}

with model_costs as (
    select 
        model_name,
        target_name,
        date_trunc('week', run_timestamp) as run_week,
        
        -- Cost metrics
        sum(execution_time_seconds / 3600.0) * 2.50 as weekly_cost_usd,
        avg(execution_time_seconds) as avg_runtime_seconds,
        count(*) as run_count,
        
        -- Efficiency metrics
        sum(rows_affected) as total_rows_processed,
        avg(execution_time_seconds / nullif(rows_affected, 0)) as seconds_per_row,
        
        -- Cost per output metrics
        (sum(execution_time_seconds / 3600.0) * 2.50) / sum(rows_affected) * 1000000 as cost_per_million_rows
        
    from {{ target.database }}.analytics.dbt_performance_log
    where run_timestamp >= current_date - 30
    group by model_name, target_name, date_trunc('week', run_timestamp)
),

cost_trends as (
    select 
        *,
        lag(weekly_cost_usd, 1) over (
            partition by model_name, target_name 
            order by run_week
        ) as prev_week_cost,
        
        -- Calculate cost trend
        case 
            when weekly_cost_usd > prev_week_cost * 1.5 then 'cost_increasing'
            when weekly_cost_usd < prev_week_cost * 0.7 then 'cost_decreasing' 
            else 'cost_stable'
        end as cost_trend,
        
        -- Optimization recommendations
        case 
            when cost_per_million_rows > 1.0 then 'High cost per row - optimize query'
            when avg_runtime_seconds > 300 and run_count > 5 then 'Long runtime - consider incremental'
            when seconds_per_row > 0.01 then 'Slow processing - review SQL efficiency'
            else 'Cost-efficient'
        end as optimization_recommendation
        
    from model_costs
),

cost_summary as (
    select 
        run_week,
        target_name,
        sum(weekly_cost_usd) as total_weekly_cost,
        count(distinct model_name) as models_with_cost,
        
        -- Top cost contributors
        max(case when rn = 1 then model_name end) as most_expensive_model,
        max(case when rn = 1 then weekly_cost_usd end) as highest_model_cost
        
    from (
        select 
            *,
            row_number() over (
                partition by run_week, target_name 
                order by weekly_cost_usd desc
            ) as rn
        from cost_trends
    ) ranked
    group by run_week, target_name
)

select 
    ct.*,
    cs.total_weekly_cost,
    cs.most_expensive_model,
    (ct.weekly_cost_usd / cs.total_weekly_cost) * 100 as pct_of_total_cost
    
from cost_trends ct
inner join cost_summary cs using (run_week, target_name)
order by run_week desc, weekly_cost_usd desc