-- ========================================
-- Timestamp-Based CDC Implementation
-- ========================================

-- Basic Timestamp CDC Model
-- models/staging/stg_customers_timestamp_cdc.sql
{{
    config(
        materialized='incremental',
        unique_key='customer_id',
        on_schema_change='sync_all_columns'
    )
}}

WITH source_data AS (
    SELECT 
        customer_id,
        customer_name,
        email,
        phone,
        address,
        status,
        created_at,
        updated_at,
        -- Add CDC processing metadata
        CURRENT_TIMESTAMP as processed_at,
        'UPSERT' as cdc_operation_type
    FROM {{ source('crm', 'customers') }}
    
    {% if is_incremental() %}
        -- Only process records updated since last successful run
        WHERE updated_at > (
            SELECT COALESCE(MAX(updated_at), '1900-01-01'::timestamp) 
            FROM {{ this }}
        )
        -- Add small buffer to handle clock skew and late-arriving data
        AND updated_at <= CURRENT_TIMESTAMP - INTERVAL '1 minute'
    {% endif %}
)

SELECT *
FROM source_data;

-- ========================================
-- Advanced Timestamp CDC with Soft Deletes
-- ========================================

-- models/staging/stg_products_advanced_timestamp_cdc.sql
{{
    config(
        materialized='incremental',
        unique_key='product_id',
        on_schema_change='sync_all_columns'
    )
}}

WITH active_products AS (
    SELECT 
        product_id,
        product_name,
        category_id,
        price,
        status,
        created_at,
        updated_at,
        FALSE as is_deleted,
        'ACTIVE' as cdc_operation_type
    FROM {{ source('catalog', 'products') }}
    WHERE status != 'DELETED'
    
    {% if is_incremental() %}
        AND updated_at > (
            SELECT COALESCE(MAX(updated_at), '1900-01-01'::timestamp) 
            FROM {{ this }}
            WHERE NOT is_deleted
        )
    {% endif %}
),

deleted_products AS (
    SELECT 
        product_id,
        product_name,
        category_id,
        price,
        status,
        created_at,
        updated_at,
        TRUE as is_deleted,
        'DELETE' as cdc_operation_type
    FROM {{ source('catalog', 'products') }}
    WHERE status = 'DELETED'
    
    {% if is_incremental() %}
        AND updated_at > (
            SELECT COALESCE(MAX(updated_at), '1900-01-01'::timestamp) 
            FROM {{ this }}
            WHERE is_deleted
        )
    {% endif %}
)

SELECT * FROM active_products
UNION ALL
SELECT * FROM deleted_products;

-- ========================================
-- Timestamp CDC with High-Water Mark State Management
-- ========================================

-- models/staging/stg_orders_watermark_cdc.sql
{{
    config(
        materialized='incremental',
        unique_key='order_id',
        pre_hook="INSERT INTO {{ this.schema }}.cdc_watermarks (table_name, last_updated_at) 
                  VALUES ('orders', CURRENT_TIMESTAMP) 
                  ON CONFLICT (table_name) DO UPDATE SET last_updated_at = EXCLUDED.last_updated_at"
    )
}}

{% set watermark_query %}
    SELECT COALESCE(MAX(last_updated_at), '1900-01-01'::timestamp) as high_water_mark
    FROM {{ this.schema }}.cdc_watermarks 
    WHERE table_name = 'orders'
{% endset %}

{% if is_incremental() %}
    {% set results = run_query(watermark_query) %}
    {% set high_water_mark = results.columns[0].values()[0] %}
{% else %}
    {% set high_water_mark = '1900-01-01'::timestamp %}
{% endif %}

WITH source_changes AS (
    SELECT 
        order_id,
        customer_id,
        order_date,
        status,
        total_amount,
        created_at,
        updated_at,
        
        -- Add CDC metadata
        '{{ high_water_mark }}' as previous_high_water_mark,
        ROW_NUMBER() OVER (
            PARTITION BY order_id 
            ORDER BY updated_at DESC
        ) as rn
    FROM {{ source('orders', 'orders') }}
    WHERE updated_at > '{{ high_water_mark }}'
      AND updated_at <= CURRENT_TIMESTAMP - INTERVAL '30 seconds'
)

SELECT *
FROM source_changes
WHERE rn = 1; -- Deduplicate within the batch

-- ========================================
-- Timestamp CDC with Timezone Handling
-- ========================================

-- models/staging/stg_events_timezone_cdc.sql
{{
    config(
        materialized='incremental',
        unique_key='event_id',
        on_schema_change='sync_all_columns'
    )
}}

WITH normalized_timestamps AS (
    SELECT 
        event_id,
        user_id,
        event_type,
        event_data,
        
        -- Normalize all timestamps to UTC
        CASE 
            WHEN source_timezone IS NOT NULL THEN
                CONVERT_TIMEZONE(source_timezone, 'UTC', event_timestamp)
            ELSE 
                event_timestamp AT TIME ZONE 'UTC'
        END as event_timestamp_utc,
        
        CASE 
            WHEN source_timezone IS NOT NULL THEN
                CONVERT_TIMEZONE(source_timezone, 'UTC', created_at)
            ELSE 
                created_at AT TIME ZONE 'UTC'
        END as created_at_utc,
        
        source_timezone,
        ingested_at
        
    FROM {{ source('events', 'user_events') }}
    
    {% if is_incremental() %}
        WHERE ingested_at > (
            SELECT COALESCE(MAX(ingested_at), '1900-01-01'::timestamp) 
            FROM {{ this }}
        )
    {% endif %}
),

-- Handle late-arriving data with event time vs processing time
processed_events AS (
    SELECT 
        *,
        -- Flag late-arriving events (more than 1 hour delay)
        CASE 
            WHEN ingested_at > event_timestamp_utc + INTERVAL '1 hour' 
            THEN TRUE 
            ELSE FALSE 
        END as is_late_arrival,
        
        -- Processing metadata
        CURRENT_TIMESTAMP as processed_at,
        EXTRACT(EPOCH FROM (ingested_at - event_timestamp_utc))/60 as processing_delay_minutes
        
    FROM normalized_timestamps
)

SELECT *
FROM processed_events;

-- ========================================
-- Timestamp CDC Performance Optimization
-- ========================================

-- models/staging/stg_transactions_optimized_cdc.sql
{{
    config(
        materialized='incremental',
        unique_key='transaction_id',
        partition_by={
            'field': 'transaction_date',
            'data_type': 'date',
            'granularity': 'day'
        },
        cluster_by=['account_id', 'transaction_type']
    )
}}

WITH partitioned_source AS (
    SELECT 
        transaction_id,
        account_id,
        transaction_type,
        amount,
        currency,
        transaction_timestamp,
        DATE(transaction_timestamp) as transaction_date,
        updated_at,
        
        -- Add content hash for efficient change detection
        {{ dbt_utils.generate_surrogate_key([
            'account_id', 'transaction_type', 'amount', 'currency'
        ]) }} as content_hash,
        
        -- Partition metadata for optimization
        DATE_TRUNC('month', transaction_timestamp) as partition_month
        
    FROM {{ source('banking', 'transactions') }}
    
    {% if is_incremental() %}
        -- Leverage partition pruning by limiting time range
        WHERE DATE(transaction_timestamp) >= CURRENT_DATE - INTERVAL '7 days'
          AND updated_at > (
              SELECT COALESCE(MAX(updated_at), '1900-01-01'::timestamp)
              FROM {{ this }}
              -- Also filter on partition for better performance
              WHERE transaction_date >= CURRENT_DATE - INTERVAL '7 days'
          )
    {% endif %}
),

-- Add derived fields for common queries
enriched_data AS (
    SELECT 
        *,
        -- Pre-calculate common aggregation keys
        {{ dbt_utils.generate_surrogate_key(['account_id', 'transaction_date']) }} as daily_account_key,
        
        -- Add business day calculations
        CASE 
            WHEN EXTRACT(DOW FROM transaction_timestamp) IN (0, 6) THEN FALSE
            ELSE TRUE
        END as is_business_day,
        
        -- Processing metadata
        CURRENT_TIMESTAMP as processed_at,
        'TIMESTAMP_CDC' as cdc_method
        
    FROM partitioned_source
)

SELECT *
FROM enriched_data;