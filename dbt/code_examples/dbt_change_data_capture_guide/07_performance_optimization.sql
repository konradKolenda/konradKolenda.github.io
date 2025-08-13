-- ========================================
-- CDC Performance Optimization Patterns
-- ========================================

-- Partition-Optimized CDC Processing
-- models/staging/stg_transactions_partition_optimized.sql
{{
    config(
        materialized='incremental',
        unique_key='transaction_id',
        partition_by={
            'field': 'transaction_date',
            'data_type': 'date',
            'granularity': 'day'
        },
        cluster_by=['account_id', 'transaction_type', 'cdc_operation'],
        on_schema_change='sync_all_columns'
    )
}}

WITH optimized_source AS (
    SELECT 
        transaction_id,
        account_id,
        transaction_type,
        amount,
        currency,
        transaction_timestamp,
        DATE(transaction_timestamp) as transaction_date,
        updated_at,
        
        -- Pre-calculate partition metadata for optimization
        DATE_TRUNC('month', transaction_timestamp) as partition_month,
        DATE_TRUNC('week', transaction_timestamp) as partition_week,
        
        -- Add content hash for efficient change detection
        {{ dbt_utils.generate_surrogate_key([
            'account_id', 'transaction_type', 'amount', 'currency'
        ]) }} as content_hash,
        
        -- Processing metadata
        'UPSERT' as cdc_operation,
        CURRENT_TIMESTAMP as processed_at
        
    FROM {{ source('banking', 'transactions') }}
    
    {% if is_incremental() %}
        -- Optimize with partition pruning
        WHERE DATE(transaction_timestamp) >= CURRENT_DATE - INTERVAL '7 days'
          AND updated_at > (
              SELECT COALESCE(MAX(updated_at), '1900-01-01'::timestamp)
              FROM {{ this }}
              WHERE transaction_date >= CURRENT_DATE - INTERVAL '7 days'
          )
    {% endif %}
),

-- Batch processing for large datasets
batched_processing AS (
    SELECT 
        *,
        -- Create processing batches to prevent memory issues
        NTILE({{ var('processing_batch_count', 20) }}) OVER (
            ORDER BY transaction_id
        ) as processing_batch,
        
        -- Pre-calculate aggregation keys for downstream performance
        {{ dbt_utils.generate_surrogate_key(['account_id', 'transaction_date']) }} as account_daily_key,
        {{ dbt_utils.generate_surrogate_key(['transaction_type', 'transaction_date']) }} as type_daily_key,
        
        -- Add derived fields commonly used in queries
        EXTRACT(HOUR FROM transaction_timestamp) as transaction_hour,
        EXTRACT(DOW FROM transaction_timestamp) as day_of_week,
        
        -- Business day flag
        CASE 
            WHEN EXTRACT(DOW FROM transaction_timestamp) BETWEEN 1 AND 5 THEN TRUE
            ELSE FALSE
        END as is_business_day
        
    FROM optimized_source
)

SELECT *
FROM batched_processing;

-- ========================================
-- Incremental Strategy Optimization
-- ========================================

-- Custom Merge Strategy for Complex CDC
-- macros/cdc_optimized_merge.sql
{% macro cdc_optimized_merge(target_relation, temp_relation, unique_key, dest_columns, cdc_column='updated_at') %}

    {% set merge_sql %}
        MERGE {{ target_relation }} as target
        USING (
            -- Pre-filter source data for efficiency
            SELECT * FROM {{ temp_relation }}
            WHERE {{ cdc_column }} IS NOT NULL
        ) as source
        ON {{ unique_key_to_sql(unique_key, 'target', 'source') }}
        
        -- Only update when source is actually newer
        WHEN MATCHED AND (
            source.{{ cdc_column }} > target.{{ cdc_column }} OR
            target.{{ cdc_column }} IS NULL
        ) THEN
            UPDATE SET
            {% for column in dest_columns -%}
                {{ column.name }} = source.{{ column.name }}
                {%- if not loop.last -%},{%- endif %}
            {%- endfor %}
        
        WHEN NOT MATCHED THEN
            INSERT (
                {%- for column in dest_columns -%}
                    {{ column.name }}
                    {%- if not loop.last -%},{%- endif %}
                {%- endfor -%}
            )
            VALUES (
                {%- for column in dest_columns -%}
                    source.{{ column.name }}
                    {%- if not loop.last -%},{%- endif %}
                {%- endfor -%}
            )
        
        -- Handle explicit deletes
        WHEN MATCHED AND source.cdc_operation = 'DELETE' THEN
            DELETE
    {% endset %}

    {{ return(merge_sql) }}

{% endmacro %}

-- ========================================
-- Memory-Optimized Large Dataset CDC
-- ========================================

-- models/staging/stg_large_table_memory_optimized.sql
{{
    config(
        materialized='incremental',
        unique_key='record_id',
        partition_by={
            'field': 'processing_date',
            'data_type': 'date', 
            'granularity': 'day'
        }
    )
}}

-- Memory optimization for large datasets
{% set max_batch_size = var('max_memory_batch_size', 50000) %}
{% set processing_date = var('processing_date', 'current_date') %}

WITH memory_optimized_batches AS (
    SELECT 
        record_id,
        entity_data,
        updated_timestamp,
        {{ processing_date }} as processing_date,
        
        -- Create smaller batches to prevent OOM
        ROW_NUMBER() OVER (ORDER BY record_id) as row_position,
        
        -- Only select essential columns initially
        CASE 
            WHEN JSON_EXTRACT_SCALAR(entity_data, '$.status') = 'DELETED' 
            THEN 'DELETE'
            ELSE 'UPSERT'
        END as operation_type
        
    FROM {{ source('large_dataset', 'entity_changes') }}
    
    {% if is_incremental() %}
        WHERE updated_timestamp > (
            SELECT COALESCE(MAX(updated_timestamp), '1900-01-01'::timestamp)
            FROM {{ this }}
            WHERE processing_date = {{ processing_date }}
        )
    {% endif %}
    
    -- Limit batch size to prevent memory issues
    LIMIT {{ max_batch_size }}
),

-- Process JSON parsing in separate step for memory efficiency
parsed_entities AS (
    SELECT 
        record_id,
        
        -- Parse JSON fields only for non-deleted records
        CASE 
            WHEN operation_type != 'DELETE' THEN 
                JSON_EXTRACT_SCALAR(entity_data, '$.name')
        END as entity_name,
        
        CASE 
            WHEN operation_type != 'DELETE' THEN 
                JSON_EXTRACT_SCALAR(entity_data, '$.category')
        END as entity_category,
        
        CASE 
            WHEN operation_type != 'DELETE' THEN 
                JSON_EXTRACT_SCALAR(entity_data, '$.value')::DECIMAL(10,2)
        END as entity_value,
        
        updated_timestamp,
        processing_date,
        operation_type,
        row_position,
        
        -- Processing metadata
        CURRENT_TIMESTAMP as processed_at
        
    FROM memory_optimized_batches
)

SELECT *
FROM parsed_entities;

-- ========================================
-- Index-Optimized CDC Query Patterns
-- ========================================

-- models/staging/stg_customers_index_optimized.sql
{{
    config(
        materialized='incremental',
        unique_key='customer_id',
        # Add indexes for common CDC query patterns
        post_hook=[
            "CREATE INDEX IF NOT EXISTS idx_customers_updated_at ON {{ this }} (updated_at)",
            "CREATE INDEX IF NOT EXISTS idx_customers_status_updated ON {{ this }} (status, updated_at)",
            "CREATE INDEX IF NOT EXISTS idx_customers_cdc_operation ON {{ this }} (cdc_operation_type, processed_at)"
        ]
    )
}}

WITH index_optimized_query AS (
    SELECT 
        customer_id,
        customer_name,
        email,
        phone,
        status,
        created_at,
        updated_at,
        
        -- Add columns commonly used in WHERE clauses
        'UPSERT' as cdc_operation_type,
        CURRENT_TIMESTAMP as processed_at,
        
        -- Optimize for common join patterns
        SUBSTR(email, POSITION('@' IN email) + 1) as email_domain,
        CASE 
            WHEN status IN ('ACTIVE', 'PREMIUM') THEN 'ACTIVE_USER'
            ELSE 'INACTIVE_USER' 
        END as user_status_category
        
    FROM {{ source('crm', 'customers') }}
    
    {% if is_incremental() %}
        -- Use index-friendly WHERE clause patterns
        WHERE updated_at > (
            SELECT MAX(updated_at) 
            FROM {{ this }}
        )
        -- Add additional filter to leverage composite indexes
        AND status IS NOT NULL
        ORDER BY updated_at, customer_id -- Optimize for index scan
    {% endif %}
)

SELECT *
FROM index_optimized_query;

-- ========================================
-- Compression and Storage Optimization
-- ========================================

-- models/staging/stg_events_storage_optimized.sql
{{
    config(
        materialized='incremental',
        unique_key='event_id',
        partition_by={
            'field': 'event_date',
            'data_type': 'date'
        },
        # Optimize storage with compression
        cluster_by=['event_type', 'user_segment'],
        # Use appropriate data types for compression
        contract={
            'enforced': true,
            'columns': [
                {'name': 'event_id', 'data_type': 'string'},
                {'name': 'user_id', 'data_type': 'integer'},
                {'name': 'event_type', 'data_type': 'string'},
                {'name': 'event_timestamp', 'data_type': 'timestamp'},
                {'name': 'event_date', 'data_type': 'date'}
            ]
        }
    )
}}

WITH storage_optimized AS (
    SELECT 
        event_id,
        user_id,
        
        -- Use enum-like values for better compression
        CASE event_type
            WHEN 'page_view' THEN 1
            WHEN 'button_click' THEN 2
            WHEN 'form_submit' THEN 3
            WHEN 'purchase' THEN 4
            ELSE 0
        END as event_type_code,
        
        event_type,
        event_timestamp,
        DATE(event_timestamp) as event_date,
        
        -- Compress JSON by extracting commonly used fields
        JSON_EXTRACT_SCALAR(event_properties, '$.page_url') as page_url,
        JSON_EXTRACT_SCALAR(event_properties, '$.referrer') as referrer,
        
        -- Store remaining properties as compressed JSON
        CASE 
            WHEN JSON_ARRAY_LENGTH(JSON_EXTRACT(event_properties, '$.custom_properties')) > 0
            THEN JSON_EXTRACT(event_properties, '$.custom_properties')
            ELSE NULL
        END as custom_properties,
        
        -- Pre-calculate user segment for clustering
        CASE 
            WHEN user_id IN (SELECT user_id FROM {{ ref('high_value_users') }}) THEN 'HIGH_VALUE'
            WHEN user_id IN (SELECT user_id FROM {{ ref('active_users') }}) THEN 'ACTIVE'
            ELSE 'STANDARD'
        END as user_segment,
        
        ingested_at
        
    FROM {{ source('events', 'raw_events') }}
    
    {% if is_incremental() %}
        WHERE ingested_at > (
            SELECT COALESCE(MAX(ingested_at), '1900-01-01'::timestamp)
            FROM {{ this }}
        )
    {% endif %}
)

SELECT *
FROM storage_optimized;

-- ========================================
-- Parallel Processing Optimization
-- ========================================

-- models/staging/stg_orders_parallel_processing.sql
{{
    config(
        materialized='incremental',
        unique_key='order_id',
        # Enable parallel processing
        partition_by={
            'field': 'order_date',
            'data_type': 'date'
        },
        cluster_by=['processing_shard']
    )
}}

-- Parallel processing by sharding data
WITH parallel_shards AS (
    SELECT 
        order_id,
        customer_id,
        order_total,
        order_status,
        order_timestamp,
        DATE(order_timestamp) as order_date,
        updated_at,
        
        -- Create processing shards for parallelization
        MOD(ABS(FARM_FINGERPRINT(CAST(order_id AS STRING))), {{ var('parallel_shards', 8) }}) as processing_shard,
        
        -- Add shard metadata for debugging
        '{{ var("parallel_shards", 8) }}' as total_shards,
        {{ var('current_shard', 0) }} as current_processing_shard
        
    FROM {{ source('ecommerce', 'orders') }}
    
    {% if is_incremental() %}
        WHERE updated_at > (
            SELECT COALESCE(MAX(updated_at), '1900-01-01'::timestamp)
            FROM {{ this }}
        )
        -- Process only assigned shard for parallel execution
        {% if var('enable_parallel_processing', false) %}
        AND MOD(ABS(FARM_FINGERPRINT(CAST(order_id AS STRING))), {{ var('parallel_shards', 8) }}) = {{ var('current_shard', 0) }}
        {% endif %}
    {% endif %}
),

-- Add processing metadata for monitoring
shard_processing AS (
    SELECT 
        *,
        COUNT(*) OVER (PARTITION BY processing_shard) as shard_record_count,
        CURRENT_TIMESTAMP as processed_at,
        
        -- Performance metrics
        CASE 
            WHEN COUNT(*) OVER (PARTITION BY processing_shard) > 10000 
            THEN 'HIGH_VOLUME_SHARD'
            ELSE 'NORMAL_SHARD'
        END as shard_load_category
        
    FROM parallel_shards
)

SELECT *
FROM shard_processing;

-- ========================================
-- Query Performance Monitoring
-- ========================================

-- models/monitoring/cdc_performance_metrics.sql
WITH processing_performance AS (
    SELECT 
        'stg_transactions_partition_optimized' as model_name,
        COUNT(*) as records_processed,
        MIN(processed_at) as batch_start_time,
        MAX(processed_at) as batch_end_time,
        COUNT(DISTINCT processing_batch) as batches_used,
        AVG(processing_batch) as avg_batch_size,
        
        -- Calculate processing rate
        COUNT(*) / GREATEST(
            EXTRACT(EPOCH FROM (MAX(processed_at) - MIN(processed_at))), 1
        ) as records_per_second,
        
        -- Memory usage indicators
        COUNT(CASE WHEN processing_batch > 15 THEN 1 END) as high_memory_batches,
        
        -- Partition efficiency
        COUNT(DISTINCT transaction_date) as partitions_processed,
        COUNT(*) / COUNT(DISTINCT transaction_date) as avg_records_per_partition
        
    FROM {{ ref('stg_transactions_partition_optimized') }}
    WHERE processed_at >= CURRENT_TIMESTAMP - INTERVAL '1 hour'
    
    UNION ALL
    
    SELECT 
        'stg_large_table_memory_optimized' as model_name,
        COUNT(*) as records_processed,
        MIN(processed_at) as batch_start_time,
        MAX(processed_at) as batch_end_time,
        1 as batches_used,
        COUNT(*) as avg_batch_size,
        COUNT(*) / GREATEST(
            EXTRACT(EPOCH FROM (MAX(processed_at) - MIN(processed_at))), 1
        ) as records_per_second,
        COUNT(CASE WHEN row_position > {{ var('max_memory_batch_size', 50000) }} * 0.8 THEN 1 END) as high_memory_batches,
        COUNT(DISTINCT processing_date) as partitions_processed,
        COUNT(*) / COUNT(DISTINCT processing_date) as avg_records_per_partition
        
    FROM {{ ref('stg_large_table_memory_optimized') }}
    WHERE processed_at >= CURRENT_TIMESTAMP - INTERVAL '1 hour'
),

performance_analysis AS (
    SELECT 
        *,
        -- Performance benchmarks
        CASE 
            WHEN records_per_second < 100 THEN 'SLOW_PROCESSING'
            WHEN records_per_second > 1000 THEN 'HIGH_PERFORMANCE'
            ELSE 'NORMAL_PERFORMANCE'
        END as performance_category,
        
        -- Resource utilization
        CASE 
            WHEN high_memory_batches > batches_used * 0.5 THEN 'HIGH_MEMORY_USAGE'
            ELSE 'NORMAL_MEMORY_USAGE'
        END as memory_utilization,
        
        -- Optimization recommendations
        CASE 
            WHEN avg_records_per_partition > 100000 THEN 'CONSIDER_SMALLER_PARTITIONS'
            WHEN avg_records_per_partition < 1000 THEN 'CONSIDER_LARGER_PARTITIONS'
            ELSE 'PARTITION_SIZE_OPTIMAL'
        END as partition_recommendation,
        
        EXTRACT(EPOCH FROM (batch_end_time - batch_start_time))/60 as processing_duration_minutes
        
    FROM processing_performance
)

SELECT 
    *,
    -- Performance score (0-100)
    LEAST(100, 
        (records_per_second / 10) + 
        (CASE WHEN performance_category = 'HIGH_PERFORMANCE' THEN 25 ELSE 0 END) +
        (CASE WHEN memory_utilization = 'NORMAL_MEMORY_USAGE' THEN 25 ELSE 0 END) +
        (CASE WHEN partition_recommendation = 'PARTITION_SIZE_OPTIMAL' THEN 25 ELSE 0 END)
    ) as performance_score,
    
    CURRENT_TIMESTAMP as analysis_timestamp
    
FROM performance_analysis;

-- ========================================
-- Cost Optimization Patterns
-- ========================================

-- models/staging/stg_cost_optimized_cdc.sql
{{
    config(
        materialized='incremental',
        unique_key='record_id',
        # Cost optimization configurations
        partition_by={
            'field': 'processing_date',
            'data_type': 'date'
        }
    )
}}

-- Cost-conscious CDC processing
WITH cost_optimized_processing AS (
    SELECT 
        record_id,
        
        -- Only process essential fields to reduce compute
        CASE 
            WHEN change_type = 'DELETE' THEN NULL
            ELSE entity_name
        END as entity_name,
        
        CASE 
            WHEN change_type = 'DELETE' THEN NULL
            ELSE status
        END as status,
        
        updated_timestamp,
        CURRENT_DATE as processing_date,
        change_type,
        
        -- Avoid expensive operations where possible
        CASE 
            WHEN change_type != 'DELETE' AND status = 'ACTIVE' 
            THEN 'PROCESS'
            ELSE 'SKIP'
        END as processing_flag
        
    FROM {{ source('cost_sensitive', 'entity_changes') }}
    
    {% if is_incremental() %}
        -- Minimize data scanned with precise filters
        WHERE processing_date = CURRENT_DATE
          AND updated_timestamp > (
              SELECT MAX(updated_timestamp)
              FROM {{ this }}
              WHERE processing_date = CURRENT_DATE
          )
    {% endif %}
)

SELECT *
FROM cost_optimized_processing
-- Only process records that need processing
WHERE processing_flag = 'PROCESS';

-- ========================================
-- Performance Testing Framework
-- ========================================

-- models/testing/cdc_performance_test.sql
WITH performance_test_scenarios AS (
    SELECT 
        'small_batch' as test_scenario,
        1000 as target_record_count,
        5 as target_duration_seconds,
        100 as min_records_per_second
        
    UNION ALL
    
    SELECT 
        'medium_batch' as test_scenario,
        10000 as target_record_count,
        30 as target_duration_seconds,
        300 as min_records_per_second
        
    UNION ALL
    
    SELECT 
        'large_batch' as test_scenario,
        100000 as target_record_count,
        120 as target_duration_seconds,
        800 as min_records_per_second
),

actual_performance AS (
    SELECT 
        'current_run' as test_run,
        COUNT(*) as actual_record_count,
        EXTRACT(EPOCH FROM (MAX(processed_at) - MIN(processed_at))) as actual_duration_seconds,
        COUNT(*) / GREATEST(
            EXTRACT(EPOCH FROM (MAX(processed_at) - MIN(processed_at))), 1
        ) as actual_records_per_second
        
    FROM {{ ref('stg_transactions_partition_optimized') }}
    WHERE processed_at >= CURRENT_TIMESTAMP - INTERVAL '1 hour'
),

performance_comparison AS (
    SELECT 
        pts.test_scenario,
        pts.target_record_count,
        ap.actual_record_count,
        pts.target_duration_seconds,
        ap.actual_duration_seconds,
        pts.min_records_per_second,
        ap.actual_records_per_second,
        
        -- Performance tests
        CASE 
            WHEN ap.actual_records_per_second >= pts.min_records_per_second THEN 'PASS'
            ELSE 'FAIL'
        END as throughput_test,
        
        CASE 
            WHEN ap.actual_duration_seconds <= pts.target_duration_seconds THEN 'PASS'
            ELSE 'FAIL'
        END as duration_test,
        
        CASE 
            WHEN ap.actual_record_count >= pts.target_record_count * 0.8 THEN 'PASS'
            ELSE 'FAIL'
        END as volume_test
        
    FROM performance_test_scenarios pts
    CROSS JOIN actual_performance ap
    WHERE pts.target_record_count <= ap.actual_record_count * 1.2 -- Match similar batch sizes
)

SELECT 
    *,
    CASE 
        WHEN throughput_test = 'PASS' AND duration_test = 'PASS' AND volume_test = 'PASS'
        THEN 'ALL_TESTS_PASSED'
        ELSE 'PERFORMANCE_ISSUES_DETECTED'
    END as overall_test_result,
    
    CURRENT_TIMESTAMP as test_timestamp
    
FROM performance_comparison;