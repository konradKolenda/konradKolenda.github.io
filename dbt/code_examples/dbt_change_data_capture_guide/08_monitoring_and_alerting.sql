-- ========================================
-- CDC Monitoring and Alerting Implementation
-- ========================================

-- Comprehensive CDC Health Monitoring
-- models/monitoring/cdc_health_dashboard.sql
WITH cdc_model_metrics AS (
    SELECT 
        model_name,
        run_started_at,
        run_completed_at,
        status,
        rows_affected,
        
        -- Calculate processing metrics
        EXTRACT(EPOCH FROM (run_completed_at - run_started_at)) as execution_time_seconds,
        rows_affected / GREATEST(EXTRACT(EPOCH FROM (run_completed_at - run_started_at)), 1) as processing_rate,
        
        -- Determine CDC method from model name
        CASE 
            WHEN model_name LIKE '%timestamp%' THEN 'TIMESTAMP_BASED'
            WHEN model_name LIKE '%log%' THEN 'LOG_BASED'
            WHEN model_name LIKE '%trigger%' THEN 'TRIGGER_BASED'
            WHEN model_name LIKE '%snapshot%' THEN 'SNAPSHOT_COMPARISON'
            ELSE 'UNKNOWN'
        END as cdc_method
        
    FROM {{ source('dbt_metadata', 'model_runs') }}
    WHERE model_name LIKE '%_cdc'
      AND run_started_at >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
),

processing_lag_analysis AS (
    SELECT 
        'timestamp_cdc' as cdc_type,
        MAX(updated_at) as latest_source_timestamp,
        MAX(processed_at) as latest_processing_timestamp,
        EXTRACT(EPOCH FROM (MAX(processed_at) - MAX(updated_at)))/60 as processing_lag_minutes
    FROM {{ ref('stg_customers_timestamp_cdc') }}
    WHERE processed_at >= CURRENT_TIMESTAMP - INTERVAL '2 hours'
    
    UNION ALL
    
    SELECT 
        'log_cdc' as cdc_type,
        MAX(commit_timestamp) as latest_source_timestamp,
        MAX(processed_at) as latest_processing_timestamp,
        EXTRACT(EPOCH FROM (MAX(processed_at) - MAX(commit_timestamp)))/60 as processing_lag_minutes
    FROM {{ ref('stg_customer_log_based_cdc') }}
    WHERE processed_at >= CURRENT_TIMESTAMP - INTERVAL '2 hours'
    
    UNION ALL
    
    SELECT 
        'trigger_cdc' as cdc_type,
        MAX(changed_at) as latest_source_timestamp,
        MAX(processed_at) as latest_processing_timestamp,
        EXTRACT(EPOCH FROM (MAX(processed_at) - MAX(changed_at)))/60 as processing_lag_minutes
    FROM {{ ref('stg_product_trigger_cdc') }}
    WHERE processed_at >= CURRENT_TIMESTAMP - INTERVAL '2 hours'
),

health_summary AS (
    SELECT 
        cmm.cdc_method,
        COUNT(*) as total_runs,
        COUNT(CASE WHEN status = 'success' THEN 1 END) as successful_runs,
        COUNT(CASE WHEN status = 'error' THEN 1 END) as failed_runs,
        AVG(execution_time_seconds) as avg_execution_time,
        AVG(processing_rate) as avg_processing_rate,
        SUM(rows_affected) as total_rows_processed,
        
        -- Health indicators
        COUNT(CASE WHEN status = 'success' THEN 1 END) * 100.0 / COUNT(*) as success_rate,
        COUNT(CASE WHEN execution_time_seconds > 300 THEN 1 END) as slow_runs,
        
        MAX(run_completed_at) as last_successful_run
        
    FROM cdc_model_metrics cmm
    GROUP BY cmm.cdc_method
)

SELECT 
    hs.*,
    pla.processing_lag_minutes,
    
    -- Overall health score (0-100)
    (
        (hs.success_rate * 0.4) +
        (CASE WHEN pla.processing_lag_minutes <= 15 THEN 30 ELSE GREATEST(0, 30 - pla.processing_lag_minutes) END) +
        (CASE WHEN hs.avg_execution_time <= 60 THEN 20 ELSE GREATEST(0, 20 - (hs.avg_execution_time - 60)/10) END) +
        (CASE WHEN hs.slow_runs = 0 THEN 10 ELSE GREATEST(0, 10 - hs.slow_runs) END)
    ) as health_score,
    
    -- Alert status
    CASE 
        WHEN hs.success_rate < 95 THEN 'CRITICAL_FAILURE_RATE'
        WHEN pla.processing_lag_minutes > 30 THEN 'CRITICAL_LAG'
        WHEN hs.success_rate < 98 THEN 'WARNING_FAILURE_RATE'
        WHEN pla.processing_lag_minutes > 15 THEN 'WARNING_LAG'
        ELSE 'HEALTHY'
    END as alert_status,
    
    CURRENT_TIMESTAMP as health_check_timestamp
    
FROM health_summary hs
LEFT JOIN processing_lag_analysis pla ON LOWER(hs.cdc_method) = REPLACE(pla.cdc_type, '_', '_based_');

-- ========================================
-- Data Quality Monitoring for CDC
-- ========================================

-- models/monitoring/cdc_data_quality_monitoring.sql
WITH duplicate_detection AS (
    -- Check for duplicates across all CDC models
    SELECT 
        'stg_customers_timestamp_cdc' as model_name,
        'customer_id' as unique_key_column,
        customer_id as key_value,
        COUNT(*) as duplicate_count,
        MAX(processed_at) as latest_processed_at
    FROM {{ ref('stg_customers_timestamp_cdc') }}
    WHERE processed_at >= CURRENT_DATE - INTERVAL '1 day'
    GROUP BY customer_id
    HAVING COUNT(*) > 1
    
    UNION ALL
    
    SELECT 
        'stg_orders_stream_cdc' as model_name,
        'order_id' as unique_key_column,
        CAST(order_id AS STRING) as key_value,
        COUNT(*) as duplicate_count,
        MAX(processed_timestamp) as latest_processed_at
    FROM {{ ref('stg_orders_stream_cdc') }}
    WHERE processed_timestamp >= CURRENT_DATE - INTERVAL '1 day'
    GROUP BY order_id
    HAVING COUNT(*) > 1
),

missing_timestamp_check AS (
    SELECT 
        'stg_customers_timestamp_cdc' as model_name,
        'updated_at' as timestamp_column,
        COUNT(*) as missing_timestamp_count
    FROM {{ ref('stg_customers_timestamp_cdc') }}
    WHERE updated_at IS NULL
      AND processed_at >= CURRENT_DATE - INTERVAL '1 day'
      
    UNION ALL
    
    SELECT 
        'stg_product_trigger_cdc' as model_name,
        'changed_at' as timestamp_column,
        COUNT(*) as missing_timestamp_count
    FROM {{ ref('stg_product_trigger_cdc') }}
    WHERE changed_at IS NULL
      AND processed_at >= CURRENT_DATE - INTERVAL '1 day'
),

future_timestamp_check AS (
    SELECT 
        'stg_customers_timestamp_cdc' as model_name,
        COUNT(*) as future_timestamp_count,
        MAX(updated_at) as max_future_timestamp
    FROM {{ ref('stg_customers_timestamp_cdc') }}
    WHERE updated_at > CURRENT_TIMESTAMP + INTERVAL '1 hour'
      AND processed_at >= CURRENT_DATE - INTERVAL '1 day'
      
    UNION ALL
    
    SELECT 
        'stg_events_microbatch_cdc' as model_name,
        COUNT(*) as future_timestamp_count,
        MAX(event_timestamp) as max_future_timestamp
    FROM {{ ref('stg_events_microbatch_cdc') }}
    WHERE event_timestamp > CURRENT_TIMESTAMP + INTERVAL '1 hour'
      AND processed_at >= CURRENT_DATE - INTERVAL '1 day'
),

sequence_gap_detection AS (
    SELECT 
        'stg_customer_log_based_cdc' as model_name,
        COUNT(*) as sequence_gaps,
        MIN(gap_start) as first_gap_sequence,
        MAX(gap_end) as last_gap_sequence
    FROM (
        SELECT 
            log_sequence_number,
            LAG(log_sequence_number) OVER (ORDER BY log_sequence_number) as prev_sequence,
            log_sequence_number as gap_start,
            LAG(log_sequence_number) OVER (ORDER BY log_sequence_number) as gap_end
        FROM {{ ref('stg_customer_log_based_cdc') }}
        WHERE processed_at >= CURRENT_DATE - INTERVAL '1 day'
    )
    WHERE log_sequence_number - prev_sequence > 1
),

data_quality_summary AS (
    SELECT 
        'DUPLICATE_KEYS' as quality_check,
        COUNT(DISTINCT model_name) as affected_models,
        SUM(duplicate_count) as total_issues,
        STRING_AGG(DISTINCT model_name, ', ') as affected_model_list
    FROM duplicate_detection
    
    UNION ALL
    
    SELECT 
        'MISSING_TIMESTAMPS' as quality_check,
        COUNT(DISTINCT model_name) as affected_models,
        SUM(missing_timestamp_count) as total_issues,
        STRING_AGG(DISTINCT model_name, ', ') as affected_model_list
    FROM missing_timestamp_check
    WHERE missing_timestamp_count > 0
    
    UNION ALL
    
    SELECT 
        'FUTURE_TIMESTAMPS' as quality_check,
        COUNT(DISTINCT model_name) as affected_models,
        SUM(future_timestamp_count) as total_issues,
        STRING_AGG(DISTINCT model_name, ', ') as affected_model_list
    FROM future_timestamp_check
    WHERE future_timestamp_count > 0
    
    UNION ALL
    
    SELECT 
        'SEQUENCE_GAPS' as quality_check,
        COUNT(DISTINCT model_name) as affected_models,
        SUM(sequence_gaps) as total_issues,
        STRING_AGG(DISTINCT model_name, ', ') as affected_model_list
    FROM sequence_gap_detection
    WHERE sequence_gaps > 0
)

SELECT 
    *,
    -- Severity assessment
    CASE 
        WHEN quality_check = 'DUPLICATE_KEYS' AND total_issues > 100 THEN 'CRITICAL'
        WHEN quality_check = 'SEQUENCE_GAPS' AND total_issues > 10 THEN 'CRITICAL'
        WHEN total_issues > 50 THEN 'HIGH'
        WHEN total_issues > 10 THEN 'MEDIUM'
        WHEN total_issues > 0 THEN 'LOW'
        ELSE 'NONE'
    END as severity,
    
    -- Recommended actions
    CASE 
        WHEN quality_check = 'DUPLICATE_KEYS' THEN 'Review unique key constraints and deduplication logic'
        WHEN quality_check = 'MISSING_TIMESTAMPS' THEN 'Investigate source data quality and add validation'
        WHEN quality_check = 'FUTURE_TIMESTAMPS' THEN 'Check system clocks and timezone handling'
        WHEN quality_check = 'SEQUENCE_GAPS' THEN 'Investigate transaction log continuity'
    END as recommended_action,
    
    CURRENT_TIMESTAMP as quality_check_timestamp
    
FROM data_quality_summary;

-- ========================================
-- SLA Monitoring and Alerting
-- ========================================

-- models/monitoring/cdc_sla_monitoring.sql
WITH sla_definitions AS (
    SELECT 'timestamp_cdc' as cdc_type, 15 as max_lag_minutes, 99.0 as min_success_rate, 120 as max_execution_seconds
    UNION ALL
    SELECT 'log_cdc' as cdc_type, 5 as max_lag_minutes, 99.5 as min_success_rate, 60 as max_execution_seconds
    UNION ALL
    SELECT 'trigger_cdc' as cdc_type, 30 as max_lag_minutes, 98.0 as min_success_rate, 300 as max_execution_seconds
    UNION ALL
    SELECT 'snapshot_cdc' as cdc_type, 60 as max_lag_minutes, 95.0 as min_success_rate, 600 as max_execution_seconds
    UNION ALL
    SELECT 'real_time_cdc' as cdc_type, 2 as max_lag_minutes, 99.9 as min_success_rate, 30 as max_execution_seconds
),

current_sla_metrics AS (
    SELECT 
        'timestamp_cdc' as cdc_type,
        EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - MAX(updated_at)))/60 as current_lag_minutes,
        95.0 as current_success_rate, -- Placeholder - would calculate from actual data
        60 as avg_execution_seconds -- Placeholder
    FROM {{ ref('stg_customers_timestamp_cdc') }}
    WHERE processed_at >= CURRENT_TIMESTAMP - INTERVAL '1 hour'
    
    UNION ALL
    
    SELECT 
        'real_time_cdc' as cdc_type,
        AVG(processing_delay_minutes) as current_lag_minutes,
        AVG(real_time_percentage) as current_success_rate,
        30 as avg_execution_seconds
    FROM {{ ref('fct_user_activity_realtime') }}
    WHERE last_processed_at >= CURRENT_TIMESTAMP - INTERVAL '1 hour'
),

sla_compliance AS (
    SELECT 
        sd.cdc_type,
        sd.max_lag_minutes,
        sd.min_success_rate,
        sd.max_execution_seconds,
        csm.current_lag_minutes,
        csm.current_success_rate,
        csm.avg_execution_seconds,
        
        -- SLA compliance checks
        CASE WHEN csm.current_lag_minutes <= sd.max_lag_minutes THEN 'PASS' ELSE 'FAIL' END as lag_sla,
        CASE WHEN csm.current_success_rate >= sd.min_success_rate THEN 'PASS' ELSE 'FAIL' END as success_rate_sla,
        CASE WHEN csm.avg_execution_seconds <= sd.max_execution_seconds THEN 'PASS' ELSE 'FAIL' END as execution_time_sla,
        
        -- Calculate SLA breach severity
        GREATEST(
            CASE WHEN csm.current_lag_minutes > sd.max_lag_minutes 
                 THEN (csm.current_lag_minutes - sd.max_lag_minutes) / sd.max_lag_minutes ELSE 0 END,
            CASE WHEN csm.current_success_rate < sd.min_success_rate 
                 THEN (sd.min_success_rate - csm.current_success_rate) / sd.min_success_rate ELSE 0 END,
            CASE WHEN csm.avg_execution_seconds > sd.max_execution_seconds 
                 THEN (csm.avg_execution_seconds - sd.max_execution_seconds) / sd.max_execution_seconds ELSE 0 END
        ) as breach_severity_ratio
        
    FROM sla_definitions sd
    LEFT JOIN current_sla_metrics csm ON sd.cdc_type = csm.cdc_type
)

SELECT 
    *,
    -- Overall SLA status
    CASE 
        WHEN lag_sla = 'PASS' AND success_rate_sla = 'PASS' AND execution_time_sla = 'PASS' THEN 'SLA_COMPLIANT'
        WHEN breach_severity_ratio > 0.5 THEN 'CRITICAL_SLA_BREACH'
        WHEN breach_severity_ratio > 0.2 THEN 'MAJOR_SLA_BREACH'
        ELSE 'MINOR_SLA_BREACH'
    END as overall_sla_status,
    
    -- Alert priority
    CASE 
        WHEN breach_severity_ratio > 0.8 THEN 'P1_CRITICAL'
        WHEN breach_severity_ratio > 0.5 THEN 'P2_HIGH'
        WHEN breach_severity_ratio > 0.2 THEN 'P3_MEDIUM'
        ELSE 'P4_LOW'
    END as alert_priority,
    
    -- Auto-generated alert message
    CASE 
        WHEN lag_sla = 'FAIL' THEN CONCAT('CDC lag exceeded: ', ROUND(current_lag_minutes, 2), ' minutes (SLA: ', max_lag_minutes, ' minutes)')
        WHEN success_rate_sla = 'FAIL' THEN CONCAT('Success rate below SLA: ', ROUND(current_success_rate, 2), '% (SLA: ', min_success_rate, '%)')
        WHEN execution_time_sla = 'FAIL' THEN CONCAT('Execution time exceeded: ', avg_execution_seconds, ' seconds (SLA: ', max_execution_seconds, ' seconds)')
        ELSE 'All SLAs met'
    END as alert_message,
    
    CURRENT_TIMESTAMP as sla_check_timestamp
    
FROM sla_compliance;

-- ========================================
-- Anomaly Detection for CDC
-- ========================================

-- models/monitoring/cdc_anomaly_detection.sql
WITH historical_baselines AS (
    SELECT 
        'customer_changes' as metric_name,
        DATE(processed_at) as metric_date,
        COUNT(*) as daily_count,
        AVG(COUNT(*)) OVER (
            ORDER BY DATE(processed_at) 
            ROWS BETWEEN 29 PRECEDING AND 1 PRECEDING
        ) as rolling_30day_avg,
        STDDEV(COUNT(*)) OVER (
            ORDER BY DATE(processed_at) 
            ROWS BETWEEN 29 PRECEDING AND 1 PRECEDING
        ) as rolling_30day_stddev
    FROM {{ ref('stg_customers_timestamp_cdc') }}
    WHERE processed_at >= CURRENT_DATE - INTERVAL '60 days'
    GROUP BY DATE(processed_at)
    
    UNION ALL
    
    SELECT 
        'order_processing_rate' as metric_name,
        DATE(processed_timestamp) as metric_date,
        AVG(EXTRACT(EPOCH FROM (processed_timestamp - order_timestamp))/60) as daily_count,
        AVG(AVG(EXTRACT(EPOCH FROM (processed_timestamp - order_timestamp))/60)) OVER (
            ORDER BY DATE(processed_timestamp) 
            ROWS BETWEEN 29 PRECEDING AND 1 PRECEDING
        ) as rolling_30day_avg,
        STDDEV(AVG(EXTRACT(EPOCH FROM (processed_timestamp - order_timestamp))/60)) OVER (
            ORDER BY DATE(processed_timestamp) 
            ROWS BETWEEN 29 PRECEDING AND 1 PRECEDING
        ) as rolling_30day_stddev
    FROM {{ ref('stg_orders_stream_cdc') }}
    WHERE processed_timestamp >= CURRENT_DATE - INTERVAL '60 days'
    GROUP BY DATE(processed_timestamp)
),

anomaly_detection AS (
    SELECT 
        metric_name,
        metric_date,
        daily_count as current_value,
        rolling_30day_avg as expected_value,
        rolling_30day_stddev,
        
        -- Calculate z-score for anomaly detection
        CASE 
            WHEN rolling_30day_stddev > 0 THEN 
                (daily_count - rolling_30day_avg) / rolling_30day_stddev
            ELSE 0
        END as z_score,
        
        -- Anomaly classification
        CASE 
            WHEN rolling_30day_stddev = 0 THEN 'NO_VARIANCE'
            WHEN ABS((daily_count - rolling_30day_avg) / rolling_30day_stddev) > 3 THEN 'EXTREME_ANOMALY'
            WHEN ABS((daily_count - rolling_30day_avg) / rolling_30day_stddev) > 2 THEN 'MODERATE_ANOMALY'
            WHEN ABS((daily_count - rolling_30day_avg) / rolling_30day_stddev) > 1.5 THEN 'MILD_ANOMALY'
            ELSE 'NORMAL'
        END as anomaly_level,
        
        -- Direction of anomaly
        CASE 
            WHEN daily_count > rolling_30day_avg + (2 * rolling_30day_stddev) THEN 'SPIKE'
            WHEN daily_count < rolling_30day_avg - (2 * rolling_30day_stddev) THEN 'DROP'
            ELSE 'NORMAL'
        END as anomaly_direction
        
    FROM historical_baselines
    WHERE metric_date >= CURRENT_DATE - INTERVAL '7 days'
      AND rolling_30day_avg IS NOT NULL
),

-- Business impact assessment
anomaly_impact AS (
    SELECT 
        *,
        -- Business impact scoring
        CASE 
            WHEN metric_name = 'customer_changes' AND anomaly_level = 'EXTREME_ANOMALY' AND anomaly_direction = 'DROP' THEN 'CRITICAL_BUSINESS_IMPACT'
            WHEN metric_name = 'order_processing_rate' AND anomaly_level IN ('EXTREME_ANOMALY', 'MODERATE_ANOMALY') THEN 'HIGH_BUSINESS_IMPACT'
            WHEN anomaly_level = 'EXTREME_ANOMALY' THEN 'MEDIUM_BUSINESS_IMPACT'
            WHEN anomaly_level = 'MODERATE_ANOMALY' THEN 'LOW_BUSINESS_IMPACT'
            ELSE 'NO_BUSINESS_IMPACT'
        END as business_impact,
        
        -- Auto-generated explanations
        CASE 
            WHEN metric_name = 'customer_changes' AND anomaly_direction = 'SPIKE' THEN 'Unusual increase in customer data changes detected'
            WHEN metric_name = 'customer_changes' AND anomaly_direction = 'DROP' THEN 'Significant decrease in customer data changes - possible data pipeline issue'
            WHEN metric_name = 'order_processing_rate' AND anomaly_direction = 'SPIKE' THEN 'Order processing latency has increased significantly'
            WHEN metric_name = 'order_processing_rate' AND anomaly_direction = 'DROP' THEN 'Order processing performance has improved unusually'
            ELSE CONCAT('Anomaly detected in ', metric_name)
        END as anomaly_explanation
        
    FROM anomaly_detection
)

SELECT 
    *,
    -- Confidence level in anomaly detection
    CASE 
        WHEN ABS(z_score) > 3 THEN 'HIGH_CONFIDENCE'
        WHEN ABS(z_score) > 2 THEN 'MEDIUM_CONFIDENCE' 
        WHEN ABS(z_score) > 1.5 THEN 'LOW_CONFIDENCE'
        ELSE 'NO_CONFIDENCE'
    END as anomaly_confidence,
    
    CURRENT_TIMESTAMP as anomaly_detection_timestamp
    
FROM anomaly_impact
WHERE anomaly_level != 'NORMAL'
ORDER BY metric_date DESC, ABS(z_score) DESC;

-- ========================================
-- Alert Configuration and Management
-- ========================================

-- models/monitoring/cdc_alert_configuration.sql
WITH alert_rules AS (
    SELECT 
        'CDC_HIGH_LAG' as alert_name,
        'Processing lag exceeds threshold' as alert_description,
        'processing_lag_minutes > 30' as alert_condition,
        'P2_HIGH' as alert_severity,
        15 as check_frequency_minutes,
        JSON_BUILD_OBJECT(
            'notification_channels', ['email', 'slack'],
            'escalation_minutes', 60,
            'auto_resolve', true
        ) as alert_config
        
    UNION ALL
    
    SELECT 
        'CDC_FAILURE_RATE' as alert_name,
        'CDC success rate below SLA' as alert_description,
        'success_rate < 95.0' as alert_condition,
        'P1_CRITICAL' as alert_severity,
        5 as check_frequency_minutes,
        JSON_BUILD_OBJECT(
            'notification_channels', ['email', 'slack', 'pagerduty'],
            'escalation_minutes', 30,
            'auto_resolve', false
        ) as alert_config
        
    UNION ALL
    
    SELECT 
        'CDC_DATA_QUALITY' as alert_name,
        'Data quality issues detected' as alert_description,
        'duplicate_count > 0 OR missing_timestamps > 0' as alert_condition,
        'P3_MEDIUM' as alert_severity,
        60 as check_frequency_minutes,
        JSON_BUILD_OBJECT(
            'notification_channels', ['email'],
            'escalation_minutes', 120,
            'auto_resolve', true
        ) as alert_config
        
    UNION ALL
    
    SELECT 
        'CDC_ANOMALY_EXTREME' as alert_name,
        'Extreme anomaly detected in CDC processing' as alert_description,
        'anomaly_level = "EXTREME_ANOMALY"' as alert_condition,
        'P2_HIGH' as alert_severity,
        30 as check_frequency_minutes,
        JSON_BUILD_OBJECT(
            'notification_channels', ['email', 'slack'],
            'escalation_minutes', 45,
            'auto_resolve', true
        ) as alert_config
),

active_alerts AS (
    SELECT 
        ar.alert_name,
        ar.alert_description,
        ar.alert_severity,
        
        -- Check current conditions (simplified - would integrate with actual monitoring data)
        CASE ar.alert_name
            WHEN 'CDC_HIGH_LAG' THEN 
                CASE WHEN (SELECT MAX(processing_lag_minutes) FROM {{ ref('cdc_health_dashboard') }}) > 30 THEN TRUE ELSE FALSE END
            WHEN 'CDC_FAILURE_RATE' THEN
                CASE WHEN (SELECT MIN(success_rate) FROM {{ ref('cdc_health_dashboard') }}) < 95 THEN TRUE ELSE FALSE END
            ELSE FALSE
        END as is_active,
        
        ar.alert_config,
        CURRENT_TIMESTAMP as alert_check_timestamp,
        
        -- Alert message generation
        CASE ar.alert_name
            WHEN 'CDC_HIGH_LAG' THEN 
                CONCAT('CDC processing lag is ', (SELECT MAX(processing_lag_minutes) FROM {{ ref('cdc_health_dashboard') }}), ' minutes')
            WHEN 'CDC_FAILURE_RATE' THEN
                CONCAT('CDC success rate is ', (SELECT MIN(success_rate) FROM {{ ref('cdc_health_dashboard') }}), '%')
            ELSE ar.alert_description
        END as alert_message
        
    FROM alert_rules ar
)

SELECT 
    *,
    -- Alert lifecycle management
    CASE 
        WHEN is_active THEN 'TRIGGERED'
        ELSE 'RESOLVED'
    END as alert_status,
    
    -- Next check time
    alert_check_timestamp + INTERVAL '1 minute' * (
        SELECT check_frequency_minutes 
        FROM alert_rules 
        WHERE alert_rules.alert_name = active_alerts.alert_name
    ) as next_check_time
    
FROM active_alerts;

-- ========================================
-- CDC Monitoring Summary Report
-- ========================================

-- models/reporting/cdc_monitoring_summary.sql
WITH monitoring_summary AS (
    SELECT 
        'CDC Health Status' as category,
        COUNT(*) as total_cdc_types,
        COUNT(CASE WHEN alert_status = 'HEALTHY' THEN 1 END) as healthy_systems,
        COUNT(CASE WHEN alert_status LIKE '%CRITICAL%' THEN 1 END) as critical_issues,
        COUNT(CASE WHEN alert_status LIKE '%WARNING%' THEN 1 END) as warning_issues,
        AVG(health_score) as avg_health_score
    FROM {{ ref('cdc_health_dashboard') }}
    
    UNION ALL
    
    SELECT 
        'Data Quality' as category,
        COUNT(DISTINCT quality_check) as total_cdc_types,
        COUNT(CASE WHEN severity = 'NONE' THEN 1 END) as healthy_systems,
        COUNT(CASE WHEN severity = 'CRITICAL' THEN 1 END) as critical_issues,
        COUNT(CASE WHEN severity IN ('HIGH', 'MEDIUM') THEN 1 END) as warning_issues,
        0 as avg_health_score
    FROM {{ ref('cdc_data_quality_monitoring') }}
    
    UNION ALL
    
    SELECT 
        'SLA Compliance' as category,
        COUNT(*) as total_cdc_types,
        COUNT(CASE WHEN overall_sla_status = 'SLA_COMPLIANT' THEN 1 END) as healthy_systems,
        COUNT(CASE WHEN overall_sla_status = 'CRITICAL_SLA_BREACH' THEN 1 END) as critical_issues,
        COUNT(CASE WHEN overall_sla_status LIKE '%BREACH%' AND overall_sla_status != 'CRITICAL_SLA_BREACH' THEN 1 END) as warning_issues,
        0 as avg_health_score
    FROM {{ ref('cdc_sla_monitoring') }}
),

executive_summary AS (
    SELECT 
        SUM(healthy_systems) as total_healthy_systems,
        SUM(critical_issues) as total_critical_issues,
        SUM(warning_issues) as total_warning_issues,
        SUM(healthy_systems + critical_issues + warning_issues) as total_monitored_systems,
        
        -- Overall health percentage
        SUM(healthy_systems) * 100.0 / NULLIF(SUM(healthy_systems + critical_issues + warning_issues), 0) as overall_health_percentage,
        
        -- System availability
        CASE 
            WHEN SUM(critical_issues) = 0 THEN 'FULLY_OPERATIONAL'
            WHEN SUM(critical_issues) <= SUM(healthy_systems) * 0.1 THEN 'MOSTLY_OPERATIONAL'
            WHEN SUM(critical_issues) <= SUM(healthy_systems) * 0.3 THEN 'PARTIALLY_OPERATIONAL'
            ELSE 'SERVICE_DEGRADED'
        END as service_status
        
    FROM monitoring_summary
)

SELECT 
    ms.*,
    es.overall_health_percentage,
    es.service_status,
    
    -- Trend indicators (simplified)
    'STABLE' as health_trend, -- Would calculate from historical data
    
    -- Next review time
    CURRENT_TIMESTAMP + INTERVAL '1 hour' as next_review_time,
    CURRENT_TIMESTAMP as report_generated_at
    
FROM monitoring_summary ms
CROSS JOIN executive_summary es;