-- Performance Optimization for dbt Semantic Layer
-- Advanced materialization and performance strategies for enterprise MetricFlow deployments

-- =============================================================================
-- MATERIALIZED AGGREGATION TABLES
-- =============================================================================

-- Daily Revenue Summary - High-frequency queries optimization
{{ config(
    materialized='incremental',
    unique_key=['date_day', 'customer_region', 'sales_channel'],
    on_schema_change='fail',
    partition_by=['date_day'],
    cluster_by=['customer_region', 'sales_channel'],
    indexes=[
        {'columns': ['date_day'], 'type': 'btree'},
        {'columns': ['customer_region', 'sales_channel'], 'type': 'btree'},
        {'columns': ['date_day', 'customer_region'], 'type': 'btree'}
    ],
    pre_hook="ANALYZE TABLE {{ this }} COMPUTE STATISTICS",
    post_hook=[
        "ANALYZE TABLE {{ this }} COMPUTE STATISTICS",
        "REFRESH MATERIALIZED VIEW mv_revenue_summary_realtime"
    ]
) }}

-- Daily aggregated metrics table for fast semantic layer queries
WITH daily_order_metrics AS (
  SELECT 
    DATE_TRUNC('day', order_date) AS date_day,
    customer_region,
    sales_channel,
    product_category,
    
    -- Core revenue metrics (pre-aggregated)
    SUM(order_total_amount) AS total_revenue,
    SUM(order_subtotal_amount) AS gross_revenue,
    SUM(tax_amount) AS total_tax,
    SUM(shipping_amount) AS total_shipping,
    SUM(discount_amount) AS total_discounts,
    
    -- Volume metrics
    COUNT(DISTINCT order_id) AS order_count,
    COUNT(DISTINCT customer_id) AS unique_customers,
    SUM(quantity_sold) AS units_sold,
    
    -- Calculated metrics
    AVG(order_total_amount) AS avg_order_value,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY order_total_amount) AS median_order_value,
    
    -- Customer segmentation counts
    COUNT(DISTINCT CASE WHEN customer_tier = 'enterprise' THEN customer_id END) AS enterprise_customers,
    COUNT(DISTINCT CASE WHEN customer_tier = 'smb' THEN customer_id END) AS smb_customers,
    COUNT(DISTINCT CASE WHEN is_first_purchase THEN customer_id END) AS new_customers,
    
    -- Channel-specific metrics
    SUM(CASE WHEN sales_channel = 'online' THEN order_total_amount ELSE 0 END) AS online_revenue,
    SUM(CASE WHEN sales_channel = 'mobile_app' THEN order_total_amount ELSE 0 END) AS mobile_revenue,
    SUM(CASE WHEN sales_channel = 'in_store' THEN order_total_amount ELSE 0 END) AS store_revenue,
    
    -- Product performance
    COUNT(DISTINCT product_id) AS active_products,
    SUM(CASE WHEN return_flag THEN 1 ELSE 0 END) AS returned_orders,
    
    -- Quality metrics
    SUM(CASE WHEN order_status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled_orders,
    AVG(customer_satisfaction_score) AS avg_satisfaction,
    
    -- Metadata for quality monitoring
    COUNT(*) AS total_records,
    MAX(updated_at) AS last_updated,
    MIN(order_date) AS min_order_date,
    MAX(order_date) AS max_order_date
    
  FROM {{ ref('fact_orders') }}
  
  {% if is_incremental() %}
    -- Incremental logic: only process new/updated data
    WHERE order_date >= (
      SELECT COALESCE(MAX(date_day), '1900-01-01') 
      FROM {{ this }}
    )
    -- Include previous day for late-arriving data
    AND order_date >= CURRENT_DATE - INTERVAL '2 days'
  {% endif %}
  
  GROUP BY 1, 2, 3, 4
),

-- Add derived calculations that require the base aggregations
enhanced_daily_metrics AS (
  SELECT 
    *,
    
    -- Calculated ratios (avoiding division by zero)
    CASE 
      WHEN order_count > 0 
      THEN total_revenue / order_count 
      ELSE NULL 
    END AS revenue_per_order,
    
    CASE 
      WHEN unique_customers > 0 
      THEN total_revenue / unique_customers 
      ELSE NULL 
    END AS revenue_per_customer,
    
    CASE 
      WHEN order_count > 0 
      THEN returned_orders::FLOAT / order_count * 100 
      ELSE NULL 
    END AS return_rate_pct,
    
    -- Channel mix percentages
    CASE 
      WHEN total_revenue > 0 
      THEN online_revenue / total_revenue * 100 
      ELSE NULL 
    END AS online_revenue_pct,
    
    CASE 
      WHEN total_revenue > 0 
      THEN mobile_revenue / total_revenue * 100 
      ELSE NULL 
    END AS mobile_revenue_pct,
    
    -- Customer acquisition metrics
    CASE 
      WHEN unique_customers > 0 
      THEN new_customers::FLOAT / unique_customers * 100 
      ELSE NULL 
    END AS new_customer_pct,
    
    -- Data quality flags
    CASE 
      WHEN total_records = 0 THEN 'NO_DATA'
      WHEN min_order_date != max_order_date THEN 'MULTI_DAY_DETECTED'
      WHEN last_updated < CURRENT_TIMESTAMP - INTERVAL '25 hours' THEN 'STALE_DATA'
      ELSE 'GOOD'
    END AS data_quality_status,
    
    -- Trend indicators (requires window functions)
    LAG(total_revenue, 1) OVER (
      PARTITION BY customer_region, sales_channel 
      ORDER BY date_day
    ) AS previous_day_revenue,
    
    LAG(total_revenue, 7) OVER (
      PARTITION BY customer_region, sales_channel 
      ORDER BY date_day
    ) AS week_ago_revenue
    
  FROM daily_order_metrics
)

SELECT * FROM enhanced_daily_metrics

-- Data quality validation
{% if is_incremental() %}
  -- Ensure we have reasonable data volumes
  HAVING COUNT(*) > 0
  AND SUM(total_revenue) > 0
{% endif %}

ORDER BY date_day DESC, customer_region, sales_channel;

-- =============================================================================
-- REAL-TIME MATERIALIZED VIEW FOR CURRENT DAY METRICS
-- =============================================================================

-- Create materialized view for real-time current day metrics
-- (This would be created separately as a materialized view)
/*
CREATE MATERIALIZED VIEW mv_revenue_summary_realtime AS
SELECT 
  CURRENT_DATE as date_day,
  customer_region,
  sales_channel,
  
  -- Real-time aggregations
  SUM(order_total_amount) AS total_revenue,
  COUNT(DISTINCT order_id) AS order_count,
  COUNT(DISTINCT customer_id) AS unique_customers,
  AVG(order_total_amount) AS avg_order_value,
  
  -- Metadata
  COUNT(*) as record_count,
  MAX(order_timestamp) as last_order_time,
  CURRENT_TIMESTAMP as refreshed_at
  
FROM {{ ref('fact_orders') }}
WHERE order_date = CURRENT_DATE
GROUP BY customer_region, sales_channel;
*/

-- =============================================================================
-- MONTHLY AGGREGATED METRICS FOR HISTORICAL ANALYSIS
-- =============================================================================

{{ config(
    materialized='table',
    partition_by=['year_month'],
    cluster_by=['customer_region'],
    indexes=[
        {'columns': ['year_month'], 'type': 'btree'},
        {'columns': ['customer_region', 'year_month'], 'type': 'btree'}
    ]
) }}

-- Monthly summary table for efficient historical trend analysis
WITH monthly_metrics AS (
  SELECT 
    DATE_TRUNC('month', date_day) AS year_month,
    customer_region,
    sales_channel,
    
    -- Aggregate from daily table for efficiency
    SUM(total_revenue) AS total_revenue,
    SUM(order_count) AS total_orders,
    SUM(unique_customers) AS total_unique_customers,
    AVG(avg_order_value) AS avg_order_value,
    
    -- Monthly-specific calculations
    COUNT(DISTINCT date_day) AS active_days,
    SUM(total_revenue) / COUNT(DISTINCT date_day) AS daily_avg_revenue,
    
    -- Customer retention metrics (requires more complex logic)
    COUNT(DISTINCT CASE WHEN new_customer_pct > 0 THEN date_day END) AS days_with_new_customers,
    
    -- Growth calculations
    LAG(SUM(total_revenue), 1) OVER (
      PARTITION BY customer_region, sales_channel 
      ORDER BY DATE_TRUNC('month', date_day)
    ) AS prev_month_revenue,
    
    -- Seasonal indicators
    EXTRACT(MONTH FROM DATE_TRUNC('month', date_day)) AS month_number,
    EXTRACT(QUARTER FROM DATE_TRUNC('month', date_day)) AS quarter_number,
    
    -- Data completeness indicators
    MIN(date_day) AS month_start,
    MAX(date_day) AS month_end,
    COUNT(*) AS days_in_data
    
  FROM {{ ref('daily_revenue_summary') }}
  GROUP BY 1, 2, 3
),

enhanced_monthly_metrics AS (
  SELECT 
    *,
    
    -- Growth rate calculations
    CASE 
      WHEN prev_month_revenue > 0 
      THEN (total_revenue - prev_month_revenue) / prev_month_revenue * 100
      ELSE NULL 
    END AS month_over_month_growth_pct,
    
    -- Seasonal classification
    CASE 
      WHEN month_number IN (11, 12) THEN 'Holiday Season'
      WHEN month_number IN (1, 2) THEN 'Post-Holiday'
      WHEN month_number IN (6, 7, 8) THEN 'Summer'
      ELSE 'Regular'
    END AS seasonal_period,
    
    -- Data completeness score
    CASE 
      WHEN days_in_data >= 28 THEN 'Complete'
      WHEN days_in_data >= 20 THEN 'Mostly Complete'
      ELSE 'Partial'
    END AS data_completeness_status
    
  FROM monthly_metrics
)

SELECT * FROM enhanced_monthly_metrics
ORDER BY year_month DESC, customer_region, sales_channel;

-- =============================================================================
-- CUSTOMER COHORT ANALYSIS OPTIMIZATION
-- =============================================================================

{{ config(
    materialized='incremental',
    unique_key='cohort_month || customer_id',
    on_schema_change='append_new_columns',
    partition_by=['cohort_month'],
    cluster_by=['customer_region'],
    indexes=[
        {'columns': ['cohort_month', 'customer_id'], 'type': 'btree'},
        {'columns': ['customer_region', 'cohort_month'], 'type': 'btree'}
    ]
) }}

-- Optimized customer cohort table for retention analysis
WITH customer_cohorts AS (
  SELECT 
    customer_id,
    customer_region,
    acquisition_channel,
    customer_tier,
    DATE_TRUNC('month', first_order_date) AS cohort_month,
    first_order_date,
    first_order_value,
    
    -- Cohort size information
    COUNT(*) OVER (PARTITION BY DATE_TRUNC('month', first_order_date)) AS cohort_size,
    
    -- Customer value metrics
    total_lifetime_value,
    total_orders,
    days_as_customer,
    
    -- Behavioral flags
    is_repeat_customer,
    has_enterprise_tier,
    has_multi_channel_purchases
    
  FROM {{ ref('dim_customers') }}
  
  {% if is_incremental() %}
    WHERE first_order_date >= (
      SELECT MAX(first_order_date) 
      FROM {{ this }}
    )
  {% endif %}
),

-- Add monthly activity for each customer
customer_monthly_activity AS (
  SELECT 
    c.customer_id,
    c.cohort_month,
    c.customer_region,
    c.acquisition_channel,
    c.cohort_size,
    
    -- Monthly purchase behavior
    DATE_TRUNC('month', o.order_date) AS activity_month,
    COUNT(DISTINCT o.order_id) AS monthly_orders,
    SUM(o.order_total_amount) AS monthly_revenue,
    
    -- Calculate months since acquisition
    DATE_DIFF('month', c.cohort_month, DATE_TRUNC('month', o.order_date)) AS months_since_acquisition,
    
    -- Retention flags
    CASE WHEN DATE_DIFF('month', c.cohort_month, DATE_TRUNC('month', o.order_date)) = 0 
         THEN 1 ELSE 0 END AS month_0_active,
    CASE WHEN DATE_DIFF('month', c.cohort_month, DATE_TRUNC('month', o.order_date)) = 1 
         THEN 1 ELSE 0 END AS month_1_active,
    CASE WHEN DATE_DIFF('month', c.cohort_month, DATE_TRUNC('month', o.order_date)) BETWEEN 2 AND 3 
         THEN 1 ELSE 0 END AS months_2_3_active,
    CASE WHEN DATE_DIFF('month', c.cohort_month, DATE_TRUNC('month', o.order_date)) BETWEEN 4 AND 6 
         THEN 1 ELSE 0 END AS months_4_6_active,
    CASE WHEN DATE_DIFF('month', c.cohort_month, DATE_TRUNC('month', o.order_date)) >= 7 
         THEN 1 ELSE 0 END AS months_7plus_active
    
  FROM customer_cohorts c
  JOIN {{ ref('fact_orders') }} o 
    ON c.customer_id = o.customer_id
  
  {% if is_incremental() %}
    WHERE o.order_date >= (
      SELECT MAX(first_order_date) 
      FROM {{ this }}
    )
  {% endif %}
  
  GROUP BY 1, 2, 3, 4, 5, 6, 9
)

SELECT * FROM customer_monthly_activity
ORDER BY cohort_month DESC, customer_id, activity_month;

-- =============================================================================
-- SEMANTIC LAYER QUERY PERFORMANCE MONITORING
-- =============================================================================

{{ config(
    materialized='incremental',
    unique_key='query_id',
    on_schema_change='append_new_columns'
) }}

-- Monitor MetricFlow query performance for optimization
WITH query_performance_log AS (
  SELECT 
    query_id,
    user_id,
    session_id,
    query_timestamp,
    
    -- Query characteristics
    requested_metrics,
    requested_dimensions,
    date_range_start,
    date_range_end,
    granularity,
    
    -- Performance metrics
    execution_time_ms,
    rows_returned,
    bytes_processed,
    cache_hit,
    
    -- Query complexity scoring
    ARRAY_LENGTH(requested_metrics) AS metric_count,
    ARRAY_LENGTH(requested_dimensions) AS dimension_count,
    DATE_DIFF('day', date_range_start, date_range_end) AS date_range_days,
    
    -- Categorize query complexity
    CASE 
      WHEN ARRAY_LENGTH(requested_metrics) > 5 OR ARRAY_LENGTH(requested_dimensions) > 3 
      THEN 'Complex'
      WHEN DATE_DIFF('day', date_range_start, date_range_end) > 365 
      THEN 'Long Range'
      ELSE 'Simple'
    END AS query_complexity,
    
    -- Performance classification
    CASE 
      WHEN execution_time_ms < 1000 THEN 'Fast'
      WHEN execution_time_ms < 10000 THEN 'Medium'
      WHEN execution_time_ms < 60000 THEN 'Slow'
      ELSE 'Very Slow'
    END AS performance_tier,
    
    -- Calculate performance scores
    CASE 
      WHEN rows_returned > 0 
      THEN execution_time_ms / rows_returned 
      ELSE execution_time_ms 
    END AS ms_per_row,
    
    CASE 
      WHEN bytes_processed > 0 
      THEN execution_time_ms / (bytes_processed / 1024 / 1024) -- MB processed
      ELSE NULL 
    END AS ms_per_mb
    
  FROM {{ source('metricflow_logs', 'query_performance') }}
  
  {% if is_incremental() %}
    WHERE query_timestamp > (
      SELECT COALESCE(MAX(query_timestamp), '1900-01-01')
      FROM {{ this }}
    )
  {% endif %}
),

-- Add rolling averages for trend analysis
performance_with_trends AS (
  SELECT 
    *,
    
    -- Rolling averages for performance trending
    AVG(execution_time_ms) OVER (
      ORDER BY query_timestamp 
      ROWS BETWEEN 99 PRECEDING AND CURRENT ROW
    ) AS rolling_100_avg_execution_time,
    
    AVG(CASE WHEN cache_hit THEN 1.0 ELSE 0.0 END) OVER (
      ORDER BY query_timestamp 
      ROWS BETWEEN 99 PRECEDING AND CURRENT ROW
    ) AS rolling_100_cache_hit_rate,
    
    -- Identify performance regressions
    LAG(execution_time_ms, 1) OVER (
      PARTITION BY requested_metrics, requested_dimensions, granularity
      ORDER BY query_timestamp
    ) AS previous_similar_query_time
    
  FROM query_performance_log
)

SELECT 
  *,
  
  -- Flag performance regressions
  CASE 
    WHEN previous_similar_query_time IS NOT NULL 
    AND execution_time_ms > previous_similar_query_time * 1.5 
    THEN TRUE 
    ELSE FALSE 
  END AS performance_regression_flag
  
FROM performance_with_trends
ORDER BY query_timestamp DESC;

-- =============================================================================
-- CACHING OPTIMIZATION QUERIES
-- =============================================================================

-- Identify most frequently queried metric combinations for cache warming
WITH metric_usage_patterns AS (
  SELECT 
    requested_metrics,
    requested_dimensions,
    granularity,
    
    -- Usage frequency
    COUNT(*) AS query_count,
    COUNT(DISTINCT user_id) AS unique_users,
    AVG(execution_time_ms) AS avg_execution_time,
    SUM(CASE WHEN cache_hit THEN 1 ELSE 0 END) AS cache_hits,
    
    -- Cache efficiency
    SUM(CASE WHEN cache_hit THEN 1 ELSE 0 END)::FLOAT / COUNT(*) AS cache_hit_rate,
    
    -- Time ranges
    MIN(date_range_start) AS earliest_date_requested,
    MAX(date_range_end) AS latest_date_requested,
    
    -- Performance impact
    SUM(execution_time_ms) AS total_execution_time,
    AVG(rows_returned) AS avg_rows_returned,
    
    -- Recency
    MAX(query_timestamp) AS last_queried,
    MIN(query_timestamp) AS first_queried
    
  FROM {{ ref('semantic_layer_query_performance') }}
  WHERE query_timestamp >= CURRENT_DATE - INTERVAL '30 days'
  GROUP BY 1, 2, 3
),

cache_recommendations AS (
  SELECT 
    *,
    
    -- Calculate cache priority score
    (
      query_count * 0.4 +  -- Frequency weight
      unique_users * 0.3 + -- User diversity weight  
      (total_execution_time / 1000) * 0.2 + -- Performance impact weight
      (1.0 - cache_hit_rate) * 0.1  -- Cache miss penalty
    ) AS cache_priority_score,
    
    -- Categorize recommendations
    CASE 
      WHEN query_count >= 100 AND cache_hit_rate < 0.5 
      THEN 'High Priority Cache'
      WHEN query_count >= 50 AND avg_execution_time > 5000 
      THEN 'Performance Cache'
      WHEN unique_users >= 10 AND cache_hit_rate < 0.7 
      THEN 'User Experience Cache'
      ELSE 'Low Priority'
    END AS cache_recommendation
    
  FROM metric_usage_patterns
)

SELECT *
FROM cache_recommendations
WHERE cache_recommendation != 'Low Priority'
ORDER BY cache_priority_score DESC
LIMIT 50;

-- =============================================================================
-- WAREHOUSE-SPECIFIC OPTIMIZATIONS
-- =============================================================================

-- Snowflake-specific optimizations
{% if target.type == 'snowflake' %}

-- Create optimized clustered table for Snowflake
CREATE OR REPLACE TABLE {{ target.schema }}.fact_orders_clustered
CLUSTER BY (order_date, customer_region)
AS SELECT * FROM {{ ref('fact_orders') }};

-- Snowflake micro-partition pruning optimization
ALTER TABLE {{ target.schema }}.daily_revenue_summary 
SET CLUSTER BY (date_day, customer_region);

-- Create search optimization for commonly filtered columns
ALTER TABLE {{ target.schema }}.daily_revenue_summary 
ADD SEARCH OPTIMIZATION ON EQUALITY(customer_region, sales_channel);

{% elif target.type == 'bigquery' %}

-- BigQuery-specific optimizations
-- Partitioning and clustering for BigQuery
CREATE OR REPLACE TABLE `{{ target.project }}.{{ target.dataset }}.fact_orders_optimized`
PARTITION BY DATE(order_date)
CLUSTER BY customer_region, sales_channel
OPTIONS(
  partition_expiration_days = 1095,  -- 3 years
  description = "Optimized orders table for semantic layer queries"
)
AS SELECT * FROM {{ ref('fact_orders') }};

-- Create materialized view for real-time aggregations
CREATE MATERIALIZED VIEW `{{ target.project }}.{{ target.dataset }}.mv_daily_revenue`
PARTITION BY date_day
CLUSTER BY customer_region
OPTIONS(
  enable_refresh = true,
  refresh_interval_minutes = 60
)
AS 
SELECT 
  DATE(order_date) as date_day,
  customer_region,
  sales_channel,
  SUM(order_total_amount) as total_revenue,
  COUNT(*) as order_count
FROM `{{ target.project }}.{{ target.dataset }}.fact_orders_optimized`
GROUP BY 1, 2, 3;

{% endif %}

/*
=============================================================================
PERFORMANCE OPTIMIZATION BEST PRACTICES SUMMARY
=============================================================================

1. MATERIALIZATION STRATEGY
   - Use incremental models for large fact tables
   - Create daily/monthly aggregation tables
   - Implement real-time materialized views for current data

2. INDEXING AND CLUSTERING
   - Index commonly filtered columns (date, region, channel)
   - Cluster tables by most selective columns
   - Use compound indexes for multi-column filters

3. PARTITIONING
   - Partition by date for time-series data
   - Use appropriate partition granularity (daily for large datasets)
   - Implement partition pruning in queries

4. CACHING OPTIMIZATION
   - Identify high-frequency query patterns
   - Pre-compute common metric combinations
   - Implement intelligent cache warming

5. QUERY OPTIMIZATION
   - Monitor query performance patterns
   - Identify and optimize slow queries
   - Use warehouse-specific optimization features

6. DATA QUALITY INTEGRATION
   - Include data quality checks in aggregation models
   - Monitor for anomalies and data issues
   - Implement automated data quality alerts

7. INCREMENTAL PROCESSING
   - Use incremental models to minimize processing time
   - Handle late-arriving data appropriately
   - Implement proper deduplication logic

8. WAREHOUSE-SPECIFIC FEATURES
   - Leverage Snowflake clustering and search optimization
   - Use BigQuery materialized views and partitioning
   - Implement Databricks Delta Lake optimizations

This comprehensive optimization strategy ensures the semantic layer
performs efficiently at enterprise scale while maintaining data quality
and governance standards.
*/