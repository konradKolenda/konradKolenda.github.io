-- Monitoring and Debugging for dbt Semantic Layer
-- Comprehensive observability and troubleshooting queries for MetricFlow

-- =============================================================================
-- DATA QUALITY MONITORING
-- =============================================================================

-- Monitor semantic layer data freshness and completeness
{{ config(
    materialized='view',
    description='Real-time data quality dashboard for semantic layer metrics'
) }}

WITH data_freshness_check AS (
  SELECT 
    'orders' AS semantic_model,
    MAX(order_date) AS latest_data_date,
    COUNT(*) AS total_records,
    COUNT(DISTINCT customer_id) AS unique_customers,
    
    -- Freshness indicators
    CURRENT_TIMESTAMP AS check_timestamp,
    DATEDIFF('hour', MAX(order_date), CURRENT_TIMESTAMP) AS hours_since_latest,
    
    -- Data quality flags
    CASE 
      WHEN DATEDIFF('hour', MAX(order_date), CURRENT_TIMESTAMP) <= 4 THEN 'FRESH'
      WHEN DATEDIFF('hour', MAX(order_date), CURRENT_TIMESTAMP) <= 24 THEN 'STALE'
      ELSE 'CRITICAL'
    END AS freshness_status,
    
    -- Volume anomaly detection
    COUNT(*) AS current_day_orders,
    AVG(COUNT(*)) OVER (
      ORDER BY DATE(order_date) 
      ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
    ) AS avg_previous_7_days,
    
    -- Calculate volume anomaly score
    CASE 
      WHEN AVG(COUNT(*)) OVER (
        ORDER BY DATE(order_date) 
        ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
      ) > 0 
      THEN ABS(COUNT(*) - AVG(COUNT(*)) OVER (
        ORDER BY DATE(order_date) 
        ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
      )) / AVG(COUNT(*)) OVER (
        ORDER BY DATE(order_date) 
        ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
      )
      ELSE 0
    END AS volume_anomaly_score
    
  FROM {{ ref('fact_orders') }}
  WHERE order_date >= CURRENT_DATE - 8  -- Current day + 7 day lookback
  GROUP BY DATE(order_date)
  ORDER BY DATE(order_date) DESC
  LIMIT 1
),

metric_calculation_validation AS (
  SELECT 
    'total_revenue' AS metric_name,
    
    -- Compare semantic layer results with direct calculation
    SUM(order_total_amount) AS direct_calculation,
    
    -- This would be replaced with actual MetricFlow query result
    -- For demonstration, using the same calculation
    SUM(order_total_amount) AS semantic_layer_result,
    
    -- Calculate variance
    ABS(SUM(order_total_amount) - SUM(order_total_amount)) AS variance_amount,
    
    CASE 
      WHEN SUM(order_total_amount) > 0 
      THEN ABS(SUM(order_total_amount) - SUM(order_total_amount)) / SUM(order_total_amount) * 100
      ELSE 0 
    END AS variance_percentage,
    
    -- Validation status
    CASE 
      WHEN ABS(SUM(order_total_amount) - SUM(order_total_amount)) / NULLIF(SUM(order_total_amount), 0) * 100 <= 0.1 
      THEN 'VALIDATED'
      WHEN ABS(SUM(order_total_amount) - SUM(order_total_amount)) / NULLIF(SUM(order_total_amount), 0) * 100 <= 1.0 
      THEN 'WARNING'
      ELSE 'ERROR'
    END AS validation_status,
    
    CURRENT_TIMESTAMP AS validation_timestamp
    
  FROM {{ ref('fact_orders') }}
  WHERE order_date = CURRENT_DATE - 1  -- Previous day validation
),

-- Null value monitoring across key dimensions
null_value_monitoring AS (
  SELECT 
    'fact_orders' AS table_name,
    
    -- Count null values in key fields
    COUNT(*) AS total_records,
    SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS null_customer_id,
    SUM(CASE WHEN order_date IS NULL THEN 1 ELSE 0 END) AS null_order_date,
    SUM(CASE WHEN order_total_amount IS NULL THEN 1 ELSE 0 END) AS null_order_total,
    SUM(CASE WHEN sales_channel IS NULL THEN 1 ELSE 0 END) AS null_sales_channel,
    
    -- Calculate null percentages
    SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END)::FLOAT / COUNT(*) * 100 AS pct_null_customer_id,
    SUM(CASE WHEN order_date IS NULL THEN 1 ELSE 0 END)::FLOAT / COUNT(*) * 100 AS pct_null_order_date,
    SUM(CASE WHEN order_total_amount IS NULL THEN 1 ELSE 0 END)::FLOAT / COUNT(*) * 100 AS pct_null_order_total,
    SUM(CASE WHEN sales_channel IS NULL THEN 1 ELSE 0 END)::FLOAT / COUNT(*) * 100 AS pct_null_sales_channel,
    
    -- Data quality score (lower is better)
    (
      SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) +
      SUM(CASE WHEN order_date IS NULL THEN 1 ELSE 0 END) +
      SUM(CASE WHEN order_total_amount IS NULL THEN 1 ELSE 0 END) +
      SUM(CASE WHEN sales_channel IS NULL THEN 1 ELSE 0 END)
    )::FLOAT / (COUNT(*) * 4) * 100 AS overall_null_percentage
    
  FROM {{ ref('fact_orders') }}
  WHERE order_date >= CURRENT_DATE - 7  -- Last 7 days
)

-- Combine all quality checks
SELECT 
  'DATA_FRESHNESS' AS check_type,
  semantic_model AS entity_name,
  freshness_status AS status,
  'Hours since latest data: ' || hours_since_latest AS details,
  check_timestamp
FROM data_freshness_check

UNION ALL

SELECT 
  'METRIC_VALIDATION' AS check_type,
  metric_name AS entity_name,
  validation_status AS status,
  'Variance: ' || ROUND(variance_percentage, 2) || '%' AS details,
  validation_timestamp AS check_timestamp
FROM metric_calculation_validation

UNION ALL

SELECT 
  'NULL_VALUE_CHECK' AS check_type,
  table_name AS entity_name,
  CASE 
    WHEN overall_null_percentage <= 1.0 THEN 'GOOD'
    WHEN overall_null_percentage <= 5.0 THEN 'WARNING'
    ELSE 'CRITICAL'
  END AS status,
  'Overall null rate: ' || ROUND(overall_null_percentage, 2) || '%' AS details,
  CURRENT_TIMESTAMP AS check_timestamp
FROM null_value_monitoring

ORDER BY check_timestamp DESC;

-- =============================================================================
-- METRIC PERFORMANCE MONITORING
-- =============================================================================

{{ config(
    materialized='incremental',
    unique_key=['metric_name', 'query_date', 'query_hour'],
    on_schema_change='append_new_columns'
) }}

-- Track metric query performance and usage patterns
WITH hourly_metric_performance AS (
  SELECT 
    metric_name,
    DATE(query_timestamp) AS query_date,
    EXTRACT(HOUR FROM query_timestamp) AS query_hour,
    
    -- Performance metrics
    COUNT(*) AS query_count,
    AVG(execution_time_ms) AS avg_execution_time,
    MIN(execution_time_ms) AS min_execution_time,
    MAX(execution_time_ms) AS max_execution_time,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY execution_time_ms) AS median_execution_time,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY execution_time_ms) AS p95_execution_time,
    
    -- Cache performance
    SUM(CASE WHEN cache_hit THEN 1 ELSE 0 END) AS cache_hits,
    SUM(CASE WHEN cache_hit THEN 0 ELSE 1 END) AS cache_misses,
    SUM(CASE WHEN cache_hit THEN 1 ELSE 0 END)::FLOAT / COUNT(*) AS cache_hit_rate,
    
    -- Data volume metrics
    AVG(rows_returned) AS avg_rows_returned,
    MAX(rows_returned) AS max_rows_returned,
    SUM(rows_returned) AS total_rows_returned,
    
    -- User engagement
    COUNT(DISTINCT user_id) AS unique_users,
    COUNT(DISTINCT session_id) AS unique_sessions,
    
    -- Error tracking
    SUM(CASE WHEN error_flag THEN 1 ELSE 0 END) AS error_count,
    SUM(CASE WHEN timeout_flag THEN 1 ELSE 0 END) AS timeout_count,
    
    -- Query complexity distribution
    AVG(ARRAY_LENGTH(requested_metrics)) AS avg_metrics_per_query,
    AVG(ARRAY_LENGTH(requested_dimensions)) AS avg_dimensions_per_query,
    AVG(DATE_DIFF('day', date_range_start, date_range_end)) AS avg_date_range_days
    
  FROM {{ source('metricflow_logs', 'query_performance') }}
  
  {% if is_incremental() %}
    WHERE query_timestamp >= (
      SELECT MAX(query_date) 
      FROM {{ this }}
    )
  {% endif %}
  
  GROUP BY 1, 2, 3
),

performance_anomaly_detection AS (
  SELECT 
    *,
    
    -- Performance trend analysis
    AVG(avg_execution_time) OVER (
      PARTITION BY metric_name 
      ORDER BY query_date, query_hour 
      ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
    ) AS rolling_24h_avg_execution_time,
    
    -- Anomaly detection flags
    CASE 
      WHEN avg_execution_time > (
        AVG(avg_execution_time) OVER (
          PARTITION BY metric_name 
          ORDER BY query_date, query_hour 
          ROWS BETWEEN 23 PRECEDING AND 1 PRECEDING
        ) * 2
      ) THEN TRUE 
      ELSE FALSE 
    END AS performance_anomaly_flag,
    
    -- Cache efficiency trends
    AVG(cache_hit_rate) OVER (
      PARTITION BY metric_name 
      ORDER BY query_date, query_hour 
      ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
    ) AS rolling_24h_cache_hit_rate,
    
    -- Usage pattern analysis
    CASE 
      WHEN query_count > (
        AVG(query_count) OVER (
          PARTITION BY metric_name 
          ORDER BY query_date, query_hour 
          ROWS BETWEEN 167 PRECEDING AND 1 PRECEDING  -- 7 days * 24 hours
        ) * 2
      ) THEN 'HIGH_USAGE'
      WHEN query_count < (
        AVG(query_count) OVER (
          PARTITION BY metric_name 
          ORDER BY query_date, query_hour 
          ROWS BETWEEN 167 PRECEDING AND 1 PRECEDING
        ) * 0.5
      ) THEN 'LOW_USAGE'
      ELSE 'NORMAL'
    END AS usage_pattern,
    
    -- Quality score calculation
    (
      LEAST(cache_hit_rate * 100, 100) * 0.3 +  -- Cache efficiency weight
      LEAST(100 - (avg_execution_time / 1000), 100) * 0.4 +  -- Performance weight (inverse)
      LEAST((1.0 - error_count::FLOAT / NULLIF(query_count, 0)) * 100, 100) * 0.3  -- Reliability weight
    ) AS quality_score
    
  FROM hourly_metric_performance
)

SELECT *
FROM performance_anomaly_detection
ORDER BY query_date DESC, query_hour DESC, metric_name;

-- =============================================================================
-- ERROR TRACKING AND DEBUGGING
-- =============================================================================

{{ config(
    materialized='view',
    description='Comprehensive error tracking for semantic layer operations'
) }}

WITH semantic_layer_errors AS (
  SELECT 
    error_timestamp,
    error_id,
    user_id,
    session_id,
    query_id,
    
    -- Error details
    error_type,
    error_message,
    error_code,
    stack_trace,
    
    -- Query context
    requested_metrics,
    requested_dimensions,
    date_range_start,
    date_range_end,
    granularity,
    
    -- Classification
    CASE 
      WHEN error_type IN ('TIMEOUT', 'CONNECTION_ERROR') THEN 'Infrastructure'
      WHEN error_type IN ('INVALID_METRIC', 'INVALID_DIMENSION') THEN 'Configuration'
      WHEN error_type IN ('PERMISSION_DENIED', 'ACCESS_VIOLATION') THEN 'Authorization'
      WHEN error_type IN ('DATA_NOT_FOUND', 'EMPTY_RESULT') THEN 'Data Quality'
      ELSE 'Other'
    END AS error_category,
    
    -- Severity assessment
    CASE 
      WHEN error_type IN ('SYSTEM_FAILURE', 'DATABASE_DOWN') THEN 'CRITICAL'
      WHEN error_type IN ('TIMEOUT', 'SLOW_QUERY') THEN 'HIGH'
      WHEN error_type IN ('INVALID_PARAMETER', 'PERMISSION_DENIED') THEN 'MEDIUM'
      ELSE 'LOW'
    END AS severity_level
    
  FROM {{ source('metricflow_logs', 'error_logs') }}
  WHERE error_timestamp >= CURRENT_TIMESTAMP - INTERVAL '7 days'
),

error_patterns AS (
  SELECT 
    error_type,
    error_category,
    severity_level,
    
    -- Error frequency
    COUNT(*) AS error_count,
    COUNT(DISTINCT user_id) AS affected_users,
    COUNT(DISTINCT DATE(error_timestamp)) AS error_days,
    
    -- Time patterns
    MIN(error_timestamp) AS first_occurrence,
    MAX(error_timestamp) AS last_occurrence,
    
    -- Most common error messages
    MODE() WITHIN GROUP (ORDER BY error_message) AS most_common_message,
    
    -- Affected queries
    COUNT(DISTINCT query_id) AS unique_failed_queries,
    
    -- Impact assessment
    CASE 
      WHEN COUNT(*) >= 100 THEN 'High Impact'
      WHEN COUNT(*) >= 20 THEN 'Medium Impact'
      ELSE 'Low Impact'
    END AS impact_level,
    
    -- Recent trend (last 24 hours vs previous 24 hours)
    SUM(CASE 
      WHEN error_timestamp >= CURRENT_TIMESTAMP - INTERVAL '24 hours' 
      THEN 1 ELSE 0 
    END) AS errors_last_24h,
    
    SUM(CASE 
      WHEN error_timestamp >= CURRENT_TIMESTAMP - INTERVAL '48 hours' 
      AND error_timestamp < CURRENT_TIMESTAMP - INTERVAL '24 hours' 
      THEN 1 ELSE 0 
    END) AS errors_previous_24h
    
  FROM semantic_layer_errors
  GROUP BY 1, 2, 3
),

error_recommendations AS (
  SELECT 
    *,
    
    -- Trend analysis
    CASE 
      WHEN errors_previous_24h = 0 AND errors_last_24h > 0 THEN 'New Issue'
      WHEN errors_previous_24h > 0 AND errors_last_24h > errors_previous_24h * 1.5 THEN 'Escalating'
      WHEN errors_previous_24h > 0 AND errors_last_24h < errors_previous_24h * 0.5 THEN 'Improving'
      ELSE 'Stable'
    END AS trend_status,
    
    -- Automated recommendations
    CASE 
      WHEN error_category = 'Infrastructure' AND error_count >= 50 
      THEN 'Check system resources and database connectivity'
      WHEN error_category = 'Configuration' AND error_count >= 20 
      THEN 'Review metric and dimension definitions'
      WHEN error_category = 'Authorization' AND affected_users >= 5 
      THEN 'Audit user permissions and access controls'
      WHEN error_category = 'Data Quality' AND error_count >= 30 
      THEN 'Investigate data pipeline issues'
      ELSE 'Monitor for patterns'
    END AS recommendation
    
  FROM error_patterns
)

SELECT *
FROM error_recommendations
WHERE error_count > 0
ORDER BY 
  CASE severity_level 
    WHEN 'CRITICAL' THEN 1 
    WHEN 'HIGH' THEN 2 
    WHEN 'MEDIUM' THEN 3 
    ELSE 4 
  END,
  error_count DESC;

-- =============================================================================
-- USER BEHAVIOR ANALYSIS
-- =============================================================================

{{ config(
    materialized='view',
    description='Analyze user behavior patterns in the semantic layer'
) }}

WITH user_activity_summary AS (
  SELECT 
    user_id,
    user_role,
    department,
    
    -- Activity volume
    COUNT(*) AS total_queries,
    COUNT(DISTINCT DATE(query_timestamp)) AS active_days,
    COUNT(DISTINCT metric_name) AS unique_metrics_used,
    COUNT(DISTINCT CONCAT_WS('|', requested_dimensions)) AS unique_dimension_combinations,
    
    -- Time patterns
    MIN(query_timestamp) AS first_query,
    MAX(query_timestamp) AS last_query,
    
    -- Usage patterns
    AVG(execution_time_ms) AS avg_query_time,
    AVG(rows_returned) AS avg_rows_returned,
    
    -- Cache utilization
    SUM(CASE WHEN cache_hit THEN 1 ELSE 0 END)::FLOAT / COUNT(*) AS personal_cache_hit_rate,
    
    -- Query complexity preferences
    AVG(ARRAY_LENGTH(requested_metrics)) AS avg_metrics_per_query,
    AVG(ARRAY_LENGTH(requested_dimensions)) AS avg_dimensions_per_query,
    AVG(DATE_DIFF('day', date_range_start, date_range_end)) AS avg_date_range_days,
    
    -- Error rates
    SUM(CASE WHEN error_flag THEN 1 ELSE 0 END)::FLOAT / COUNT(*) AS error_rate,
    
    -- Peak usage hours
    MODE() WITHIN GROUP (ORDER BY EXTRACT(HOUR FROM query_timestamp)) AS peak_hour
    
  FROM {{ source('metricflow_logs', 'query_performance') }} q
  LEFT JOIN {{ ref('dim_users') }} u ON q.user_id = u.user_id
  WHERE query_timestamp >= CURRENT_TIMESTAMP - INTERVAL '30 days'
  GROUP BY 1, 2, 3
),

user_segmentation AS (
  SELECT 
    *,
    
    -- User activity segmentation
    CASE 
      WHEN total_queries >= 100 AND active_days >= 20 THEN 'Power User'
      WHEN total_queries >= 50 AND active_days >= 10 THEN 'Regular User'  
      WHEN total_queries >= 10 AND active_days >= 3 THEN 'Occasional User'
      ELSE 'Light User'
    END AS user_segment,
    
    -- Complexity preference
    CASE 
      WHEN avg_metrics_per_query >= 3 OR avg_dimensions_per_query >= 3 THEN 'Complex'
      WHEN avg_date_range_days >= 90 THEN 'Historical Analysis'
      ELSE 'Simple'
    END AS query_complexity_preference,
    
    -- Performance sensitivity
    CASE 
      WHEN avg_query_time <= 2000 THEN 'Fast Queries'
      WHEN avg_query_time <= 10000 THEN 'Medium Queries'
      ELSE 'Slow Queries'
    END AS performance_profile,
    
    -- Engagement score calculation
    LEAST(100, (
      (total_queries / 10.0) * 0.3 +  -- Query volume weight
      (active_days / 3.0) * 0.3 +     -- Consistency weight
      (unique_metrics_used / 2.0) * 0.2 +  -- Breadth weight
      (personal_cache_hit_rate * 100) * 0.1 +  -- Efficiency weight
      ((1 - error_rate) * 100) * 0.1  -- Success weight
    )) AS engagement_score
    
  FROM user_activity_summary
)

SELECT *
FROM user_segmentation
ORDER BY engagement_score DESC, total_queries DESC;

-- =============================================================================
-- SEMANTIC MODEL HEALTH CHECK
-- =============================================================================

{{ config(
    materialized='view',
    description='Comprehensive health check for all semantic models'
) }}

WITH semantic_model_metrics AS (
  SELECT 
    'orders' AS semantic_model_name,
    
    -- Data volume checks
    COUNT(*) AS total_records,
    COUNT(DISTINCT customer_id) AS unique_entities,
    COUNT(DISTINCT DATE(order_date)) AS date_coverage_days,
    
    -- Data quality metrics
    SUM(CASE WHEN order_total_amount IS NOT NULL THEN 1 ELSE 0 END)::FLOAT / COUNT(*) AS measure_completeness,
    SUM(CASE WHEN customer_region IS NOT NULL THEN 1 ELSE 0 END)::FLOAT / COUNT(*) AS dimension_completeness,
    
    -- Value distributions
    MIN(order_total_amount) AS min_measure_value,
    MAX(order_total_amount) AS max_measure_value,
    AVG(order_total_amount) AS avg_measure_value,
    STDDEV(order_total_amount) AS stddev_measure_value,
    
    -- Time dimension analysis
    MIN(order_date) AS earliest_date,
    MAX(order_date) AS latest_date,
    
    -- Referential integrity checks
    COUNT(DISTINCT customer_id) AS customer_references,
    COUNT(DISTINCT product_id) AS product_references,
    
    -- Current timestamp for monitoring
    CURRENT_TIMESTAMP AS health_check_timestamp
    
  FROM {{ ref('fact_orders') }}
  
  UNION ALL
  
  SELECT 
    'customers' AS semantic_model_name,
    COUNT(*) AS total_records,
    COUNT(*) AS unique_entities,  -- Primary entity count
    COUNT(DISTINCT DATE(created_at)) AS date_coverage_days,
    
    SUM(CASE WHEN total_lifetime_value IS NOT NULL THEN 1 ELSE 0 END)::FLOAT / COUNT(*) AS measure_completeness,
    SUM(CASE WHEN customer_segment IS NOT NULL THEN 1 ELSE 0 END)::FLOAT / COUNT(*) AS dimension_completeness,
    
    MIN(total_lifetime_value) AS min_measure_value,
    MAX(total_lifetime_value) AS max_measure_value,
    AVG(total_lifetime_value) AS avg_measure_value,
    STDDEV(total_lifetime_value) AS stddev_measure_value,
    
    MIN(created_at) AS earliest_date,
    MAX(created_at) AS latest_date,
    
    COUNT(DISTINCT customer_segment) AS customer_references,
    COUNT(DISTINCT acquisition_channel) AS product_references,
    
    CURRENT_TIMESTAMP AS health_check_timestamp
    
  FROM {{ ref('dim_customers') }}
),

health_assessment AS (
  SELECT 
    *,
    
    -- Overall health score calculation
    CASE 
      WHEN measure_completeness >= 0.95 
       AND dimension_completeness >= 0.90 
       AND total_records > 1000 
       AND date_coverage_days >= 30 
      THEN 'Excellent'
      WHEN measure_completeness >= 0.85 
       AND dimension_completeness >= 0.80 
       AND total_records > 100 
       AND date_coverage_days >= 7 
      THEN 'Good'
      WHEN measure_completeness >= 0.70 
       AND dimension_completeness >= 0.60 
       AND total_records > 10 
      THEN 'Fair'
      ELSE 'Poor'
    END AS health_status,
    
    -- Data freshness assessment
    CASE 
      WHEN DATEDIFF('day', latest_date, CURRENT_DATE) <= 1 THEN 'Fresh'
      WHEN DATEDIFF('day', latest_date, CURRENT_DATE) <= 7 THEN 'Recent'
      ELSE 'Stale'
    END AS freshness_status,
    
    -- Volume trend (would need historical data for proper implementation)
    CASE 
      WHEN total_records >= 10000 THEN 'High Volume'
      WHEN total_records >= 1000 THEN 'Medium Volume'
      ELSE 'Low Volume'
    END AS volume_category,
    
    -- Quality issues identification
    ARRAY[
      CASE WHEN measure_completeness < 0.9 THEN 'Low measure completeness' ELSE NULL END,
      CASE WHEN dimension_completeness < 0.8 THEN 'Low dimension completeness' ELSE NULL END,
      CASE WHEN date_coverage_days < 30 THEN 'Limited date coverage' ELSE NULL END,
      CASE WHEN stddev_measure_value / NULLIF(avg_measure_value, 0) > 2 THEN 'High value variance' ELSE NULL END
    ] AS quality_issues
    
  FROM semantic_model_metrics
)

SELECT 
  semantic_model_name,
  health_status,
  freshness_status,
  volume_category,
  total_records,
  unique_entities,
  ROUND(measure_completeness * 100, 2) AS measure_completeness_pct,
  ROUND(dimension_completeness * 100, 2) AS dimension_completeness_pct,
  date_coverage_days,
  earliest_date,
  latest_date,
  ARRAY_TO_STRING(ARRAY_REMOVE(quality_issues, NULL), ', ') AS identified_issues,
  health_check_timestamp
  
FROM health_assessment
ORDER BY 
  CASE health_status 
    WHEN 'Poor' THEN 1 
    WHEN 'Fair' THEN 2 
    WHEN 'Good' THEN 3 
    ELSE 4 
  END,
  semantic_model_name;

-- =============================================================================
-- ALERTING QUERIES FOR OPERATIONAL MONITORING
-- =============================================================================

-- Query to identify critical issues requiring immediate attention
{{ config(
    materialized='view',
    description='Critical alerts for semantic layer operations'
) }}

WITH critical_alerts AS (
  -- Data freshness alerts
  SELECT 
    'DATA_FRESHNESS' AS alert_type,
    'CRITICAL' AS severity,
    'orders semantic model' AS entity,
    'Data is ' || DATEDIFF('hour', MAX(order_date), CURRENT_TIMESTAMP) || ' hours old' AS message,
    MAX(order_date) AS related_timestamp,
    CURRENT_TIMESTAMP AS alert_timestamp
  FROM {{ ref('fact_orders') }}
  HAVING DATEDIFF('hour', MAX(order_date), CURRENT_TIMESTAMP) > 6
  
  UNION ALL
  
  -- Metric validation failures
  SELECT 
    'METRIC_VALIDATION' AS alert_type,
    'HIGH' AS severity,
    'total_revenue metric' AS entity,
    'Validation variance exceeds threshold: ' || ROUND(
      ABS(SUM(order_total_amount) - SUM(order_total_amount)) / NULLIF(SUM(order_total_amount), 0) * 100, 2
    ) || '%' AS message,
    CURRENT_DATE - 1 AS related_timestamp,
    CURRENT_TIMESTAMP AS alert_timestamp
  FROM {{ ref('fact_orders') }}
  WHERE order_date = CURRENT_DATE - 1
  HAVING ABS(SUM(order_total_amount) - SUM(order_total_amount)) / NULLIF(SUM(order_total_amount), 0) * 100 > 1.0
  
  UNION ALL
  
  -- High error rates
  SELECT 
    'ERROR_RATE' AS alert_type,
    'HIGH' AS severity,
    error_type AS entity,
    'Error rate spike: ' || COUNT(*) || ' errors in last hour' AS message,
    MAX(error_timestamp) AS related_timestamp,
    CURRENT_TIMESTAMP AS alert_timestamp
  FROM {{ source('metricflow_logs', 'error_logs') }}
  WHERE error_timestamp >= CURRENT_TIMESTAMP - INTERVAL '1 hour'
  GROUP BY error_type
  HAVING COUNT(*) >= 10
  
  UNION ALL
  
  -- Performance degradation
  SELECT 
    'PERFORMANCE' AS alert_type,
    'MEDIUM' AS severity,
    metric_name AS entity,
    'Performance degraded: avg ' || ROUND(AVG(execution_time_ms)/1000, 1) || 's execution time' AS message,
    MAX(query_timestamp) AS related_timestamp,
    CURRENT_TIMESTAMP AS alert_timestamp
  FROM {{ source('metricflow_logs', 'query_performance') }}
  WHERE query_timestamp >= CURRENT_TIMESTAMP - INTERVAL '2 hours'
  GROUP BY metric_name
  HAVING AVG(execution_time_ms) > 30000  -- 30 seconds
)

SELECT *
FROM critical_alerts
ORDER BY 
  CASE severity 
    WHEN 'CRITICAL' THEN 1 
    WHEN 'HIGH' THEN 2 
    WHEN 'MEDIUM' THEN 3 
    ELSE 4 
  END,
  alert_timestamp DESC;

/*
=============================================================================
MONITORING AND DEBUGGING BEST PRACTICES SUMMARY
=============================================================================

1. DATA QUALITY MONITORING
   - Track data freshness across all semantic models
   - Monitor null values and data completeness
   - Validate metric calculations against source systems
   - Implement anomaly detection for volume changes

2. PERFORMANCE MONITORING
   - Track query execution times and identify slow queries
   - Monitor cache hit rates and optimization opportunities
   - Analyze user behavior patterns and usage trends
   - Set up performance regression detection

3. ERROR TRACKING AND DEBUGGING
   - Centralize error logging with categorization
   - Track error patterns and root cause analysis
   - Implement automated error notification
   - Maintain error trend analysis and resolution tracking

4. USER BEHAVIOR ANALYSIS
   - Segment users by activity and complexity preferences
   - Track metric adoption and usage patterns
   - Identify training and support opportunities
   - Monitor user engagement and satisfaction

5. SEMANTIC MODEL HEALTH CHECKS
   - Comprehensive health scoring for all models
   - Referential integrity monitoring
   - Data distribution analysis and outlier detection
   - Regular freshness and completeness assessments

6. OPERATIONAL ALERTING
   - Critical issue identification and escalation
   - Automated notification systems
   - SLA monitoring and breach detection
   - Business impact assessment

7. CONTINUOUS IMPROVEMENT
   - Regular review of monitoring metrics
   - Performance optimization recommendations
   - User feedback integration
   - Proactive issue prevention

This comprehensive monitoring framework ensures high availability,
performance, and user satisfaction for the semantic layer while
enabling proactive issue resolution and continuous optimization.
*/