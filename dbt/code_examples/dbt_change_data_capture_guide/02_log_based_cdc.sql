-- ========================================
-- Log-Based CDC Implementation
-- ========================================

-- Processing WAL/Binlog Stream Data
-- models/staging/stg_customer_log_based_cdc.sql
{{
    config(
        materialized='incremental',
        unique_key=['customer_id', 'log_sequence_number'],
        on_schema_change='sync_all_columns'
    )
}}

WITH raw_log_entries AS (
    SELECT 
        -- Log metadata
        log_sequence_number,
        transaction_id,
        commit_timestamp,
        operation_type, -- INSERT, UPDATE, DELETE
        table_name,
        
        -- Parse the changed data from JSON
        JSON_EXTRACT_SCALAR(change_data, '$.customer_id')::INTEGER as customer_id,
        
        -- Extract before/after values for UPDATE operations
        CASE operation_type
            WHEN 'UPDATE' THEN JSON_EXTRACT(change_data, '$.before')
            WHEN 'DELETE' THEN JSON_EXTRACT(change_data, '$.before') 
            ELSE NULL
        END as before_values,
        
        CASE operation_type
            WHEN 'DELETE' THEN JSON_EXTRACT(change_data, '$.before')
            ELSE JSON_EXTRACT(change_data, '$.after')
        END as after_values,
        
        -- Full change record for audit
        change_data,
        ingested_at
        
    FROM {{ source('cdc_logs', 'customer_changes') }}
    WHERE table_name = 'customers'
    
    {% if is_incremental() %}
        AND log_sequence_number > (
            SELECT COALESCE(MAX(log_sequence_number), 0) 
            FROM {{ this }}
        )
    {% endif %}
),

-- Extract structured data from JSON
parsed_changes AS (
    SELECT 
        log_sequence_number,
        transaction_id,
        commit_timestamp,
        operation_type,
        customer_id,
        
        -- Parse current values based on operation type
        CASE 
            WHEN operation_type = 'DELETE' THEN NULL
            ELSE JSON_EXTRACT_SCALAR(after_values, '$.customer_name')
        END as customer_name,
        
        CASE 
            WHEN operation_type = 'DELETE' THEN NULL
            ELSE JSON_EXTRACT_SCALAR(after_values, '$.email')
        END as email,
        
        CASE 
            WHEN operation_type = 'DELETE' THEN NULL
            ELSE JSON_EXTRACT_SCALAR(after_values, '$.phone')
        END as phone,
        
        CASE 
            WHEN operation_type = 'DELETE' THEN NULL
            ELSE JSON_EXTRACT_SCALAR(after_values, '$.address')
        END as address,
        
        -- Store previous values for UPDATE operations
        CASE 
            WHEN operation_type = 'UPDATE' THEN JSON_EXTRACT_SCALAR(before_values, '$.customer_name')
            ELSE NULL
        END as previous_customer_name,
        
        CASE 
            WHEN operation_type = 'UPDATE' THEN JSON_EXTRACT_SCALAR(before_values, '$.email')
            ELSE NULL
        END as previous_email,
        
        -- Metadata
        before_values,
        after_values,
        change_data,
        ingested_at,
        CURRENT_TIMESTAMP as processed_at
        
    FROM raw_log_entries
),

-- Handle transaction boundaries and ordering
transaction_ordered AS (
    SELECT 
        *,
        -- Determine final state within transaction
        LAST_VALUE(operation_type) OVER (
            PARTITION BY customer_id, transaction_id 
            ORDER BY log_sequence_number 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) as final_operation_in_tx,
        
        -- Mark the latest change per customer per transaction
        ROW_NUMBER() OVER (
            PARTITION BY customer_id, transaction_id 
            ORDER BY log_sequence_number DESC
        ) = 1 as is_final_in_transaction
        
    FROM parsed_changes
)

SELECT *
FROM transaction_ordered;

-- ========================================
-- Log-Based CDC Current State Reconstruction
-- ========================================

-- models/marts/dim_customers_current_state.sql
-- Reconstruct current customer state from log entries
{{
    config(
        materialized='incremental',
        unique_key='customer_id',
        incremental_strategy='merge'
    )
}}

WITH latest_changes AS (
    SELECT 
        customer_id,
        customer_name,
        email,
        phone,
        address,
        operation_type,
        commit_timestamp,
        log_sequence_number,
        
        -- Get the most recent change per customer
        ROW_NUMBER() OVER (
            PARTITION BY customer_id 
            ORDER BY log_sequence_number DESC
        ) = 1 as is_latest_change
        
    FROM {{ ref('stg_customer_log_based_cdc') }}
    WHERE is_final_in_transaction = TRUE
    
    {% if is_incremental() %}
        -- Only process customers that had changes since last run
        AND customer_id IN (
            SELECT DISTINCT customer_id 
            FROM {{ ref('stg_customer_log_based_cdc') }}
            WHERE processed_at > (
                SELECT COALESCE(MAX(last_updated_at), '1900-01-01'::timestamp)
                FROM {{ this }}
            )
        )
    {% endif %}
),

current_customers AS (
    SELECT 
        customer_id,
        customer_name,
        email,
        phone,
        address,
        commit_timestamp as last_updated_at,
        log_sequence_number as last_sequence_number,
        
        -- Mark deleted customers
        CASE WHEN operation_type = 'DELETE' THEN TRUE ELSE FALSE END as is_deleted,
        
        -- Audit metadata
        operation_type as last_operation,
        CURRENT_TIMESTAMP as reconstructed_at
        
    FROM latest_changes
    WHERE is_latest_change = TRUE
)

SELECT *
FROM current_customers
-- Exclude deleted records from the current state view
WHERE NOT is_deleted;

-- ========================================
-- Log-Based CDC with Conflict Resolution
-- ========================================

-- models/staging/stg_inventory_log_cdc_resolved.sql
{{
    config(
        materialized='incremental',
        unique_key='inventory_id',
        on_schema_change='sync_all_columns'
    )
}}

WITH log_changes AS (
    SELECT 
        JSON_EXTRACT_SCALAR(change_data, '$.inventory_id')::INTEGER as inventory_id,
        JSON_EXTRACT_SCALAR(change_data, '$.product_id')::INTEGER as product_id,
        JSON_EXTRACT_SCALAR(change_data, '$.location_id')::INTEGER as location_id,
        JSON_EXTRACT_SCALAR(change_data, '$.quantity')::INTEGER as quantity,
        
        log_sequence_number,
        transaction_id,
        commit_timestamp,
        operation_type,
        
        -- Extract the user who made the change
        JSON_EXTRACT_SCALAR(change_data, '$.changed_by') as changed_by,
        
        ingested_at
        
    FROM {{ source('cdc_logs', 'inventory_changes') }}
    
    {% if is_incremental() %}
        WHERE log_sequence_number > (
            SELECT COALESCE(MAX(log_sequence_number), 0) 
            FROM {{ this }}
        )
    {% endif %}
),

-- Detect and resolve conflicts (concurrent updates to same inventory)
conflict_resolution AS (
    SELECT 
        *,
        -- Detect overlapping transactions
        CASE 
            WHEN COUNT(*) OVER (
                PARTITION BY inventory_id 
                ORDER BY commit_timestamp 
                RANGE BETWEEN INTERVAL '1 second' PRECEDING 
                          AND INTERVAL '1 second' FOLLOWING
            ) > 1 THEN TRUE
            ELSE FALSE
        END as has_concurrent_update,
        
        -- Apply conflict resolution rule: latest commit timestamp wins
        ROW_NUMBER() OVER (
            PARTITION BY inventory_id, 
                         DATE_TRUNC('second', commit_timestamp)
            ORDER BY log_sequence_number DESC
        ) = 1 as is_winning_change,
        
        -- Calculate time gaps between changes
        LAG(commit_timestamp) OVER (
            PARTITION BY inventory_id 
            ORDER BY log_sequence_number
        ) as previous_change_timestamp
        
    FROM log_changes
),

-- Apply business rules for conflict resolution
resolved_changes AS (
    SELECT 
        *,
        -- Flag suspicious rapid changes (< 1 second apart)
        CASE 
            WHEN previous_change_timestamp IS NOT NULL 
             AND EXTRACT(EPOCH FROM (commit_timestamp - previous_change_timestamp)) < 1
            THEN TRUE
            ELSE FALSE
        END as is_rapid_change,
        
        -- Resolution metadata
        CASE 
            WHEN has_concurrent_update AND is_winning_change THEN 'CONFLICT_RESOLVED_LATEST_WINS'
            WHEN has_concurrent_update AND NOT is_winning_change THEN 'CONFLICT_RESOLVED_SUPERSEDED'
            ELSE 'NO_CONFLICT'
        END as conflict_resolution_status,
        
        CURRENT_TIMESTAMP as processed_at
        
    FROM conflict_resolution
    WHERE is_winning_change = TRUE -- Only keep winning changes
)

SELECT *
FROM resolved_changes;

-- ========================================
-- Log-Based CDC Performance Optimization
-- ========================================

-- models/staging/stg_orders_log_optimized.sql
{{
    config(
        materialized='incremental',
        unique_key='order_id',
        partition_by={
            'field': 'commit_date',
            'data_type': 'date',
            'granularity': 'day'
        },
        cluster_by=['customer_id', 'order_status']
    )
}}

WITH batched_log_processing AS (
    SELECT 
        JSON_EXTRACT_SCALAR(change_data, '$.order_id')::BIGINT as order_id,
        JSON_EXTRACT_SCALAR(change_data, '$.customer_id')::INTEGER as customer_id,
        JSON_EXTRACT_SCALAR(change_data, '$.order_status') as order_status,
        JSON_EXTRACT_SCALAR(change_data, '$.total_amount')::DECIMAL(10,2) as total_amount,
        
        log_sequence_number,
        transaction_id,
        commit_timestamp,
        DATE(commit_timestamp) as commit_date,
        operation_type,
        
        -- Batch processing metadata
        FLOOR(log_sequence_number / 10000) as processing_batch,
        
        -- Pre-calculate aggregation keys for performance
        CONCAT(
            JSON_EXTRACT_SCALAR(change_data, '$.customer_id'), 
            '_', 
            DATE(commit_timestamp)
        ) as customer_daily_key,
        
        change_data,
        ingested_at
        
    FROM {{ source('cdc_logs', 'order_changes') }}
    
    {% if is_incremental() %}
        -- Process in smaller batches to avoid memory issues
        WHERE log_sequence_number > (
            SELECT COALESCE(MAX(log_sequence_number), 0) 
            FROM {{ this }}
        )
        -- Limit batch size for large tables
        AND log_sequence_number <= (
            SELECT COALESCE(MAX(log_sequence_number), 0) + 50000
            FROM {{ this }}
        )
    {% endif %}
),

-- Optimize deduplication with window functions
deduplicated_changes AS (
    SELECT 
        *,
        -- Efficient deduplication within processing batch
        ROW_NUMBER() OVER (
            PARTITION BY order_id 
            ORDER BY log_sequence_number DESC,
                     CASE operation_type 
                         WHEN 'DELETE' THEN 3
                         WHEN 'UPDATE' THEN 2  
                         WHEN 'INSERT' THEN 1
                     END DESC
        ) = 1 as is_latest_change_in_batch
        
    FROM batched_log_processing
)

SELECT *
FROM deduplicated_changes
WHERE is_latest_change_in_batch = TRUE;

-- ========================================
-- Log-Based CDC Audit Trail
-- ========================================

-- models/audit/customer_change_history.sql
-- Complete audit trail from log-based CDC
{{
    config(
        materialized='incremental',
        unique_key=['customer_id', 'log_sequence_number'],
        on_schema_change='sync_all_columns'
    )
}}

WITH change_analysis AS (
    SELECT 
        customer_id,
        log_sequence_number,
        transaction_id,
        commit_timestamp,
        operation_type,
        
        -- Current values
        customer_name,
        email,
        phone,
        address,
        
        -- Previous values (for UPDATE operations)
        previous_customer_name,
        previous_email,
        
        -- Change detection
        CASE 
            WHEN operation_type = 'INSERT' THEN 'NEW_CUSTOMER'
            WHEN operation_type = 'DELETE' THEN 'CUSTOMER_DELETED'
            WHEN previous_customer_name != customer_name THEN 'NAME_CHANGED'
            WHEN previous_email != email THEN 'EMAIL_CHANGED'
            ELSE 'OTHER_CHANGE'
        END as change_type,
        
        -- Calculate change frequency
        LAG(commit_timestamp) OVER (
            PARTITION BY customer_id 
            ORDER BY log_sequence_number
        ) as previous_change_time,
        
        processed_at
        
    FROM {{ ref('stg_customer_log_based_cdc') }}
    
    {% if is_incremental() %}
        WHERE processed_at > (
            SELECT COALESCE(MAX(processed_at), '1900-01-01'::timestamp)
            FROM {{ this }}
        )
    {% endif %}
),

enriched_history AS (
    SELECT 
        *,
        -- Time between changes
        CASE 
            WHEN previous_change_time IS NOT NULL THEN
                EXTRACT(EPOCH FROM (commit_timestamp - previous_change_time))/3600
            ELSE NULL
        END as hours_since_last_change,
        
        -- Change velocity (changes per day)
        COUNT(*) OVER (
            PARTITION BY customer_id 
            ORDER BY commit_timestamp 
            RANGE BETWEEN INTERVAL '24 hours' PRECEDING AND CURRENT ROW
        ) as changes_in_last_24h,
        
        -- Audit metadata
        CURRENT_TIMESTAMP as audit_processed_at
        
    FROM change_analysis
)

SELECT *
FROM enriched_history;