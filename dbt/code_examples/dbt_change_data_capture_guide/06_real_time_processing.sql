-- ========================================
-- Real-Time CDC Processing Implementation
-- ========================================

-- Micro-Batch Processing for Real-Time CDC
-- models/staging/stg_events_microbatch_cdc.sql
{{
    config(
        materialized='incremental',
        incremental_strategy='microbatch',
        event_time='event_timestamp',
        batch_size='hour',
        unique_key='event_id',
        lookback=2
    )
}}

WITH event_stream AS (
    SELECT 
        event_id,
        user_id,
        session_id,
        event_type,
        event_timestamp,
        
        -- Parse event payload
        JSON_EXTRACT_SCALAR(event_data, '$.page_url') as page_url,
        JSON_EXTRACT_SCALAR(event_data, '$.referrer') as referrer,
        JSON_EXTRACT_SCALAR(event_data, '$.user_agent') as user_agent,
        JSON_EXTRACT_SCALAR(event_data, '$.ip_address') as ip_address,
        
        -- Batch processing metadata
        DATE_TRUNC('hour', event_timestamp) as batch_hour,
        ingested_at,
        
        -- Calculate event ordering within user session
        ROW_NUMBER() OVER (
            PARTITION BY user_id, session_id 
            ORDER BY event_timestamp
        ) as event_sequence_in_session,
        
        -- Time since last event for session detection
        LAG(event_timestamp) OVER (
            PARTITION BY user_id 
            ORDER BY event_timestamp
        ) as previous_event_time
        
    FROM {{ source('events', 'user_events') }}
    WHERE event_timestamp >= '{{ var("start_time") }}'
      AND event_timestamp < '{{ var("end_time") }}'
),

-- Process events with real-time business logic
processed_events AS (
    SELECT 
        *,
        -- Calculate time between events
        COALESCE(
            EXTRACT(EPOCH FROM (event_timestamp - previous_event_time))/60,
            0
        ) as minutes_since_last_event,
        
        -- Detect session boundaries (30+ minutes of inactivity)
        CASE 
            WHEN previous_event_time IS NULL THEN 'SESSION_START'
            WHEN EXTRACT(EPOCH FROM (event_timestamp - previous_event_time)) > 1800 THEN 'SESSION_START'
            ELSE 'SESSION_CONTINUE'
        END as session_indicator,
        
        -- Real-time event classification
        CASE 
            WHEN event_type = 'page_view' AND page_url LIKE '%/product/%' THEN 'PRODUCT_VIEW'
            WHEN event_type = 'page_view' AND page_url LIKE '%/checkout%' THEN 'CHECKOUT_VIEW'
            WHEN event_type = 'click' AND page_url LIKE '%/cart%' THEN 'CART_INTERACTION'
            WHEN event_type = 'purchase' THEN 'CONVERSION'
            ELSE 'OTHER'
        END as business_event_type,
        
        -- Add processing metadata
        CURRENT_TIMESTAMP as processed_at,
        '{{ var("start_time") }}' as batch_start_time,
        '{{ var("end_time") }}' as batch_end_time
        
    FROM event_stream
)

SELECT *
FROM processed_events;

-- ========================================
-- Real-Time Streaming Aggregations
-- ========================================

-- models/marts/fct_user_activity_realtime.sql
{{
    config(
        materialized='incremental',
        unique_key=['user_id', 'activity_hour'],
        on_schema_change='sync_all_columns'
    )
}}

WITH hourly_user_activity AS (
    SELECT 
        user_id,
        batch_hour as activity_hour,
        
        -- Volume metrics
        COUNT(*) as total_events,
        COUNT(DISTINCT session_id) as unique_sessions,
        COUNT(DISTINCT event_type) as unique_event_types,
        
        -- Engagement metrics
        COUNT(CASE WHEN business_event_type = 'PRODUCT_VIEW' THEN 1 END) as product_views,
        COUNT(CASE WHEN business_event_type = 'CART_INTERACTION' THEN 1 END) as cart_interactions,
        COUNT(CASE WHEN business_event_type = 'CHECKOUT_VIEW' THEN 1 END) as checkout_views,
        COUNT(CASE WHEN business_event_type = 'CONVERSION' THEN 1 END) as conversions,
        
        -- Session metrics
        COUNT(CASE WHEN session_indicator = 'SESSION_START' THEN 1 END) as new_sessions_started,
        MAX(event_sequence_in_session) as max_events_in_session,
        AVG(event_sequence_in_session) as avg_events_per_session,
        
        -- Time-based metrics
        MIN(event_timestamp) as first_event_time,
        MAX(event_timestamp) as last_event_time,
        EXTRACT(EPOCH FROM (MAX(event_timestamp) - MIN(event_timestamp)))/60 as activity_duration_minutes,
        
        -- Real-time indicators
        COUNT(CASE WHEN EXTRACT(EPOCH FROM (ingested_at - event_timestamp)) < 60 THEN 1 END) as real_time_events,
        MAX(ingested_at) as last_ingested_at,
        MAX(processed_at) as last_processed_at
        
    FROM {{ ref('stg_events_microbatch_cdc') }}
    
    {% if is_incremental() %}
        WHERE batch_hour > (
            SELECT COALESCE(MAX(activity_hour), '1900-01-01'::timestamp)
            FROM {{ this }}
        )
    {% endif %}
    
    GROUP BY user_id, batch_hour
),

-- Add derived metrics and alerts
enriched_activity AS (
    SELECT 
        *,
        -- Calculate conversion rates
        CASE 
            WHEN product_views > 0 THEN conversions * 100.0 / product_views 
            ELSE 0 
        END as product_to_conversion_rate,
        
        CASE 
            WHEN checkout_views > 0 THEN conversions * 100.0 / checkout_views 
            ELSE 0 
        END as checkout_to_conversion_rate,
        
        -- Engagement scoring
        (product_views * 1 + cart_interactions * 2 + checkout_views * 3 + conversions * 10) as engagement_score,
        
        -- Real-time processing quality
        real_time_events * 100.0 / total_events as real_time_percentage,
        EXTRACT(EPOCH FROM (last_processed_at - activity_hour))/60 as processing_delay_minutes,
        
        -- User behavior flags
        CASE 
            WHEN conversions > 0 THEN 'CONVERTER'
            WHEN checkout_views > 0 THEN 'HIGH_INTENT'
            WHEN cart_interactions > 0 THEN 'MEDIUM_INTENT'
            WHEN product_views > 0 THEN 'BROWSING'
            ELSE 'LOW_ENGAGEMENT'
        END as user_behavior_segment,
        
        CURRENT_TIMESTAMP as aggregation_timestamp
        
    FROM hourly_user_activity
)

SELECT *
FROM enriched_activity;

-- ========================================
-- Real-Time CDC Stream Processing
-- ========================================

-- models/staging/stg_orders_stream_cdc.sql
{{
    config(
        materialized='incremental',
        unique_key='order_id',
        on_schema_change='sync_all_columns'
    )
}}

WITH streaming_order_changes AS (
    SELECT 
        order_id,
        customer_id,
        order_status,
        order_total,
        currency,
        order_timestamp,
        updated_timestamp,
        
        -- Stream processing metadata
        stream_offset,
        partition_id,
        ingested_at,
        
        -- Parse change type from stream metadata
        JSON_EXTRACT_SCALAR(stream_metadata, '$.operation') as operation_type,
        JSON_EXTRACT_SCALAR(stream_metadata, '$.source_db') as source_database,
        JSON_EXTRACT_SCALAR(stream_metadata, '$.transaction_id') as source_transaction_id,
        
        -- Calculate processing latency
        EXTRACT(EPOCH FROM (ingested_at - updated_timestamp))/60 as ingestion_latency_minutes
        
    FROM {{ source('streaming', 'order_changes_stream') }}
    
    {% if is_incremental() %}
        WHERE stream_offset > (
            SELECT COALESCE(MAX(stream_offset), 0) 
            FROM {{ this }}
        )
        -- Process in smaller batches for real-time processing
        AND stream_offset <= (
            SELECT COALESCE(MAX(stream_offset), 0) + 1000
            FROM {{ this }}
        )
    {% endif %}
),

-- Apply real-time business rules and enrichment
enriched_stream_data AS (
    SELECT 
        *,
        -- Real-time order classification
        CASE 
            WHEN order_total >= 1000 THEN 'HIGH_VALUE'
            WHEN order_total >= 100 THEN 'MEDIUM_VALUE'
            ELSE 'LOW_VALUE'
        END as order_value_segment,
        
        -- Fraud detection flags (simple rules)
        CASE 
            WHEN order_total > 5000 AND 
                 EXTRACT(EPOCH FROM (order_timestamp - LAG(order_timestamp) OVER (PARTITION BY customer_id ORDER BY order_timestamp)))/60 < 5
            THEN 'POTENTIAL_FRAUD_HIGH_FREQUENCY'
            WHEN order_total > 10000 THEN 'POTENTIAL_FRAUD_HIGH_VALUE'
            ELSE 'NORMAL'
        END as fraud_risk_flag,
        
        -- Order velocity metrics
        COUNT(*) OVER (
            PARTITION BY customer_id 
            ORDER BY order_timestamp 
            RANGE BETWEEN INTERVAL '1 hour' PRECEDING AND CURRENT ROW
        ) as orders_last_hour,
        
        -- Stream processing quality metrics
        CASE 
            WHEN ingestion_latency_minutes <= 1 THEN 'REAL_TIME'
            WHEN ingestion_latency_minutes <= 5 THEN 'NEAR_REAL_TIME'
            ELSE 'BATCH'
        END as processing_timeliness,
        
        CURRENT_TIMESTAMP as processed_timestamp
        
    FROM streaming_order_changes
),

-- Deduplicate and apply conflict resolution
deduplicated_orders AS (
    SELECT 
        *,
        -- Handle potential duplicates from stream processing
        ROW_NUMBER() OVER (
            PARTITION BY order_id 
            ORDER BY stream_offset DESC, updated_timestamp DESC
        ) = 1 as is_latest_update
        
    FROM enriched_stream_data
)

SELECT *
FROM deduplicated_orders
WHERE is_latest_update = TRUE;

-- ========================================
-- Real-Time CDC Monitoring and Alerting
-- ========================================

-- models/monitoring/realtime_cdc_health.sql
WITH stream_health_metrics AS (
    SELECT 
        'order_stream' as stream_name,
        DATE_TRUNC('minute', processed_timestamp) as processing_minute,
        
        -- Volume metrics
        COUNT(*) as messages_processed,
        COUNT(DISTINCT order_id) as unique_orders,
        COUNT(DISTINCT customer_id) as unique_customers,
        
        -- Latency metrics
        AVG(ingestion_latency_minutes) as avg_latency_minutes,
        MAX(ingestion_latency_minutes) as max_latency_minutes,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY ingestion_latency_minutes) as p95_latency_minutes,
        
        -- Quality metrics
        COUNT(CASE WHEN processing_timeliness = 'REAL_TIME' THEN 1 END) as real_time_messages,
        COUNT(CASE WHEN fraud_risk_flag != 'NORMAL' THEN 1 END) as flagged_orders,
        
        -- Processing metadata
        MIN(stream_offset) as min_offset_processed,
        MAX(stream_offset) as max_offset_processed,
        MAX(processed_timestamp) as last_processed_at
        
    FROM {{ ref('stg_orders_stream_cdc') }}
    WHERE processed_timestamp >= CURRENT_TIMESTAMP - INTERVAL '1 hour'
    GROUP BY processing_minute
),

-- Generate alerts for real-time processing issues
processing_alerts AS (
    SELECT 
        *,
        -- SLA violations
        CASE 
            WHEN avg_latency_minutes > 5 THEN 'HIGH_LATENCY'
            WHEN messages_processed = 0 THEN 'NO_MESSAGES'
            WHEN real_time_messages * 100.0 / messages_processed < 80 THEN 'LOW_REAL_TIME_PERCENTAGE'
            ELSE 'HEALTHY'
        END as health_status,
        
        -- Throughput analysis
        messages_processed - LAG(messages_processed, 1, 0) OVER (
            ORDER BY processing_minute
        ) as throughput_change,
        
        -- Gap detection
        CASE 
            WHEN processing_minute - LAG(processing_minute) OVER (ORDER BY processing_minute) > INTERVAL '2 minutes'
            THEN 'PROCESSING_GAP'
            ELSE 'CONTINUOUS'
        END as continuity_status
        
    FROM stream_health_metrics
),

-- User activity real-time metrics
activity_health AS (
    SELECT 
        'user_activity' as stream_name,
        activity_hour as processing_minute,
        SUM(total_events) as messages_processed,
        COUNT(DISTINCT user_id) as unique_users,
        COUNT(DISTINCT user_id) as unique_customers,
        AVG(processing_delay_minutes) as avg_latency_minutes,
        MAX(processing_delay_minutes) as max_latency_minutes,
        AVG(processing_delay_minutes) as p95_latency_minutes, -- Simplified
        SUM(CASE WHEN real_time_percentage >= 80 THEN total_events ELSE 0 END) as real_time_messages,
        0 as flagged_orders,
        0 as min_offset_processed,
        0 as max_offset_processed,
        MAX(last_processed_at) as last_processed_at,
        
        CASE 
            WHEN AVG(processing_delay_minutes) > 10 THEN 'HIGH_LATENCY'
            WHEN SUM(total_events) = 0 THEN 'NO_MESSAGES'
            WHEN AVG(real_time_percentage) < 70 THEN 'LOW_REAL_TIME_PERCENTAGE'
            ELSE 'HEALTHY'
        END as health_status,
        
        0 as throughput_change,
        'CONTINUOUS' as continuity_status
        
    FROM {{ ref('fct_user_activity_realtime') }}
    WHERE activity_hour >= CURRENT_TIMESTAMP - INTERVAL '2 hours'
      AND last_processed_at >= CURRENT_TIMESTAMP - INTERVAL '1 hour'
    GROUP BY activity_hour
)

SELECT * FROM processing_alerts
WHERE health_status != 'HEALTHY'
   OR continuity_status != 'CONTINUOUS'

UNION ALL

SELECT * FROM activity_health
WHERE health_status != 'HEALTHY';

-- ========================================
-- Real-Time CDC Performance Optimization
-- ========================================

-- models/staging/stg_high_volume_stream_optimized.sql
{{
    config(
        materialized='incremental',
        unique_key='message_id',
        partition_by={
            'field': 'processing_date',
            'data_type': 'date',
            'granularity': 'day'
        },
        cluster_by=['stream_partition', 'message_type']
    )
}}

WITH optimized_stream_processing AS (
    SELECT 
        message_id,
        stream_partition,
        message_offset,
        message_type,
        payload_data,
        message_timestamp,
        DATE(message_timestamp) as processing_date,
        
        -- Optimize JSON parsing with pre-computed paths
        JSON_EXTRACT_SCALAR(payload_data, '$.entity_id') as entity_id,
        JSON_EXTRACT_SCALAR(payload_data, '$.operation') as operation_type,
        JSON_EXTRACT_SCALAR(payload_data, '$.table_name') as source_table,
        
        -- Batch processing optimization
        FLOOR(message_offset / {{ var('stream_batch_size', 5000) }}) as processing_batch,
        
        -- Pre-calculate common aggregation keys
        CONCAT(
            JSON_EXTRACT_SCALAR(payload_data, '$.entity_id'),
            '_',
            DATE(message_timestamp)
        ) as entity_daily_key,
        
        ingested_at,
        CURRENT_TIMESTAMP as processed_at
        
    FROM {{ source('streams', 'high_volume_changes') }}
    
    {% if is_incremental() %}
        WHERE message_offset > (
            SELECT COALESCE(MAX(message_offset), 0) 
            FROM {{ this }}
            WHERE processing_date >= CURRENT_DATE - INTERVAL '1 day'
        )
        -- Limit batch size to prevent memory issues
        AND message_offset <= (
            SELECT COALESCE(MAX(message_offset), 0) + {{ var('max_batch_size', 10000) }}
            FROM {{ this }}
        )
    {% endif %}
),

-- Parallel processing by partition
parallel_processed AS (
    SELECT 
        *,
        -- Efficient deduplication within partition
        ROW_NUMBER() OVER (
            PARTITION BY entity_id, stream_partition
            ORDER BY message_offset DESC
        ) = 1 as is_latest_in_partition,
        
        -- Processing performance metrics
        EXTRACT(EPOCH FROM (processed_at - ingested_at)) as processing_latency_seconds,
        
        -- Memory optimization flags
        CASE 
            WHEN LENGTH(payload_data::TEXT) > 10000 THEN 'LARGE_PAYLOAD'
            ELSE 'NORMAL_PAYLOAD'
        END as payload_size_category
        
    FROM optimized_stream_processing
)

SELECT *
FROM parallel_processed
WHERE is_latest_in_partition = TRUE;

-- ========================================
-- Real-Time CDC State Management
-- ========================================

-- models/marts/current_state_realtime.sql
{{
    config(
        materialized='incremental',
        unique_key='entity_id',
        incremental_strategy='merge'
    )
}}

WITH latest_entity_states AS (
    SELECT 
        entity_id,
        source_table,
        operation_type,
        message_timestamp,
        payload_data,
        processing_batch,
        processed_at,
        
        -- Get the absolute latest state per entity across all partitions
        ROW_NUMBER() OVER (
            PARTITION BY entity_id 
            ORDER BY message_offset DESC
        ) = 1 as is_absolute_latest
        
    FROM {{ ref('stg_high_volume_stream_optimized') }}
    
    {% if is_incremental() %}
        WHERE entity_id IN (
            SELECT DISTINCT entity_id 
            FROM {{ ref('stg_high_volume_stream_optimized') }}
            WHERE processed_at > (
                SELECT COALESCE(MAX(last_updated_at), '1900-01-01'::timestamp)
                FROM {{ this }}
            )
        )
    {% endif %}
),

reconstructed_state AS (
    SELECT 
        entity_id,
        source_table,
        
        -- Reconstruct current state from latest message
        CASE 
            WHEN operation_type = 'DELETE' THEN NULL
            ELSE JSON_EXTRACT_SCALAR(payload_data, '$.current_state.name')
        END as entity_name,
        
        CASE 
            WHEN operation_type = 'DELETE' THEN NULL
            ELSE JSON_EXTRACT_SCALAR(payload_data, '$.current_state.status')
        END as entity_status,
        
        CASE 
            WHEN operation_type = 'DELETE' THEN NULL
            ELSE JSON_EXTRACT_SCALAR(payload_data, '$.current_state.value')::DECIMAL(10,2)
        END as entity_value,
        
        -- State metadata
        operation_type as last_operation,
        message_timestamp as last_updated_at,
        processed_at as last_processed_at,
        
        -- Entity lifecycle
        CASE 
            WHEN operation_type = 'DELETE' THEN TRUE
            ELSE FALSE
        END as is_deleted,
        
        -- Processing metadata
        processing_batch as last_processing_batch,
        CURRENT_TIMESTAMP as state_reconstructed_at
        
    FROM latest_entity_states
    WHERE is_absolute_latest = TRUE
)

SELECT *
FROM reconstructed_state
-- Optionally exclude deleted entities for current state view
WHERE NOT is_deleted;

-- ========================================
-- Real-Time CDC Error Handling
-- ========================================

-- models/monitoring/realtime_cdc_errors.sql
WITH processing_errors AS (
    SELECT 
        'stream_processing' as error_source,
        entity_id,
        message_offset,
        stream_partition,
        error_message,
        error_timestamp,
        payload_data,
        
        -- Categorize errors
        CASE 
            WHEN error_message LIKE '%JSON%' THEN 'JSON_PARSE_ERROR'
            WHEN error_message LIKE '%timeout%' THEN 'TIMEOUT_ERROR'
            WHEN error_message LIKE '%duplicate%' THEN 'DUPLICATE_KEY_ERROR'
            WHEN error_message LIKE '%schema%' THEN 'SCHEMA_ERROR'
            ELSE 'OTHER_ERROR'
        END as error_category,
        
        -- Retry information
        retry_count,
        last_retry_at,
        
        -- Error severity
        CASE 
            WHEN retry_count >= 3 THEN 'CRITICAL'
            WHEN error_category IN ('TIMEOUT_ERROR', 'SCHEMA_ERROR') THEN 'HIGH'
            ELSE 'MEDIUM'
        END as error_severity
        
    FROM {{ source('monitoring', 'stream_processing_errors') }}
    WHERE error_timestamp >= CURRENT_TIMESTAMP - INTERVAL '1 hour'
),

error_summary AS (
    SELECT 
        error_source,
        error_category,
        error_severity,
        COUNT(*) as error_count,
        COUNT(DISTINCT entity_id) as affected_entities,
        MIN(error_timestamp) as first_error_time,
        MAX(error_timestamp) as last_error_time,
        AVG(retry_count) as avg_retry_count,
        
        -- Impact assessment
        CASE 
            WHEN COUNT(*) > 100 THEN 'HIGH_VOLUME_ERRORS'
            WHEN COUNT(DISTINCT entity_id) > 50 THEN 'WIDESPREAD_IMPACT'
            WHEN MAX(retry_count) >= 3 THEN 'PERSISTENT_FAILURES'
            ELSE 'MANAGEABLE'
        END as impact_level
        
    FROM processing_errors
    GROUP BY error_source, error_category, error_severity
)

SELECT 
    *,
    -- Generate recommendations
    CASE 
        WHEN error_category = 'JSON_PARSE_ERROR' THEN 'Check payload schema validation'
        WHEN error_category = 'TIMEOUT_ERROR' THEN 'Increase processing timeout or reduce batch size'
        WHEN error_category = 'DUPLICATE_KEY_ERROR' THEN 'Review deduplication logic'
        WHEN error_category = 'SCHEMA_ERROR' THEN 'Update schema migration process'
        ELSE 'Investigate error details'
    END as recommended_action,
    
    CURRENT_TIMESTAMP as analysis_timestamp
    
FROM error_summary
WHERE impact_level != 'MANAGEABLE' 
   OR error_severity = 'CRITICAL';