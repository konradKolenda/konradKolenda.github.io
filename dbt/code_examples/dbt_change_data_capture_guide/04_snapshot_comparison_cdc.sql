-- ========================================
-- Snapshot Comparison CDC Implementation
-- ========================================

-- Full Snapshot Comparison Model
-- models/staging/stg_suppliers_snapshot_cdc.sql
{{
    config(
        materialized='incremental',
        unique_key='supplier_id',
        on_schema_change='sync_all_columns'
    )
}}

{% if is_incremental() %}
-- Incremental run: Compare current snapshot with previous
WITH current_snapshot AS (
    SELECT 
        supplier_id,
        supplier_name,
        contact_email,
        phone_number,
        address,
        city,
        country,
        credit_rating,
        payment_terms,
        status,
        CURRENT_DATE as snapshot_date,
        
        -- Generate content hash for change detection
        {{ dbt_utils.generate_surrogate_key([
            'supplier_name', 'contact_email', 'phone_number', 
            'address', 'city', 'country', 'credit_rating', 
            'payment_terms', 'status'
        ]) }} as content_hash
    FROM {{ source('procurement', 'suppliers') }}
),

previous_snapshot AS (
    SELECT 
        supplier_id,
        supplier_name,
        contact_email,
        phone_number,
        address,
        city,
        country,
        credit_rating,
        payment_terms,
        status,
        content_hash as previous_content_hash,
        snapshot_date as previous_snapshot_date
    FROM {{ this }}
    WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM {{ this }})
),

-- Identify changes by comparing snapshots
change_detection AS (
    SELECT 
        COALESCE(c.supplier_id, p.supplier_id) as supplier_id,
        c.supplier_name,
        c.contact_email,
        c.phone_number,
        c.address,
        c.city,
        c.country,
        c.credit_rating,
        c.payment_terms,
        c.status,
        CURRENT_DATE as snapshot_date,
        c.content_hash,
        p.previous_content_hash,
        
        -- Determine change type
        CASE 
            WHEN p.supplier_id IS NULL THEN 'INSERT'
            WHEN c.supplier_id IS NULL THEN 'DELETE'
            WHEN c.content_hash != p.previous_content_hash THEN 'UPDATE'
            ELSE 'NO_CHANGE'
        END as change_type,
        
        -- Track specific field changes for UPDATEs
        CASE WHEN p.supplier_name != c.supplier_name THEN TRUE ELSE FALSE END as name_changed,
        CASE WHEN p.contact_email != c.contact_email THEN TRUE ELSE FALSE END as email_changed,
        CASE WHEN p.credit_rating != c.credit_rating THEN TRUE ELSE FALSE END as credit_rating_changed,
        
        -- Store previous values for audit
        p.supplier_name as previous_supplier_name,
        p.contact_email as previous_contact_email,
        p.credit_rating as previous_credit_rating,
        p.previous_snapshot_date
        
    FROM current_snapshot c
    FULL OUTER JOIN previous_snapshot p ON c.supplier_id = p.supplier_id
),

-- Handle deleted records
deleted_suppliers AS (
    SELECT 
        supplier_id,
        previous_supplier_name as supplier_name,
        previous_contact_email as contact_email,
        NULL as phone_number,
        NULL as address,
        NULL as city,
        NULL as country,
        previous_credit_rating as credit_rating,
        NULL as payment_terms,
        'DELETED' as status,
        CURRENT_DATE as snapshot_date,
        NULL as content_hash,
        previous_content_hash,
        'DELETE' as change_type,
        FALSE as name_changed,
        FALSE as email_changed,
        FALSE as credit_rating_changed,
        previous_supplier_name,
        previous_contact_email,
        previous_credit_rating,
        previous_snapshot_date
    FROM change_detection
    WHERE change_type = 'DELETE'
),

-- Combine all changes
all_changes AS (
    SELECT * FROM change_detection WHERE change_type != 'NO_CHANGE' AND change_type != 'DELETE'
    UNION ALL
    SELECT * FROM deleted_suppliers
)

SELECT 
    *,
    -- Add processing metadata
    CURRENT_TIMESTAMP as processed_at,
    'SNAPSHOT_COMPARISON' as cdc_method,
    
    -- Generate change summary
    CASE 
        WHEN change_type = 'INSERT' THEN 'New supplier added'
        WHEN change_type = 'DELETE' THEN 'Supplier removed'
        WHEN name_changed AND email_changed THEN 'Name and email updated'
        WHEN name_changed THEN 'Name updated'
        WHEN email_changed THEN 'Email updated'
        WHEN credit_rating_changed THEN 'Credit rating updated'
        ELSE 'Other fields updated'
    END as change_summary
FROM all_changes

{% else %}
-- Initial load: Load all current data
SELECT 
    supplier_id,
    supplier_name,
    contact_email,
    phone_number,
    address,
    city,
    country,
    credit_rating,
    payment_terms,
    status,
    CURRENT_DATE as snapshot_date,
    {{ dbt_utils.generate_surrogate_key([
        'supplier_name', 'contact_email', 'phone_number', 
        'address', 'city', 'country', 'credit_rating', 
        'payment_terms', 'status'
    ]) }} as content_hash,
    NULL as previous_content_hash,
    'INITIAL_LOAD' as change_type,
    FALSE as name_changed,
    FALSE as email_changed,
    FALSE as credit_rating_changed,
    NULL as previous_supplier_name,
    NULL as previous_contact_email,
    NULL as previous_credit_rating,
    NULL as previous_snapshot_date,
    CURRENT_TIMESTAMP as processed_at,
    'SNAPSHOT_COMPARISON' as cdc_method,
    'Initial load' as change_summary
FROM {{ source('procurement', 'suppliers') }}

{% endif %}

-- ========================================
-- Optimized Snapshot Comparison with Partitioning
-- ========================================

-- models/staging/stg_customers_snapshot_optimized.sql
{{
    config(
        materialized='incremental',
        unique_key=['customer_id', 'snapshot_date'],
        partition_by={
            'field': 'snapshot_date',
            'data_type': 'date',
            'granularity': 'day'
        },
        cluster_by=['customer_id', 'change_type']
    )
}}

{% if is_incremental() %}

WITH current_snapshot AS (
    SELECT 
        customer_id,
        customer_name,
        email,
        phone,
        address,
        customer_segment,
        annual_revenue,
        status,
        CURRENT_DATE as snapshot_date,
        
        -- Create hash for efficient comparison
        {{ dbt_utils.generate_surrogate_key([
            'customer_name', 'email', 'phone', 'address', 
            'customer_segment', 'annual_revenue', 'status'
        ]) }} as content_hash,
        
        -- Add row number for large dataset optimization
        ROW_NUMBER() OVER (ORDER BY customer_id) as row_num
        
    FROM {{ source('crm', 'customers') }}
    WHERE status != 'ARCHIVED' -- Exclude archived customers
),

-- Optimize previous snapshot retrieval with partitioning
previous_snapshot AS (
    SELECT 
        customer_id,
        content_hash as previous_content_hash,
        customer_name as previous_customer_name,
        customer_segment as previous_customer_segment,
        annual_revenue as previous_annual_revenue
    FROM {{ this }}
    WHERE snapshot_date = (
        SELECT MAX(snapshot_date) 
        FROM {{ this }} 
        WHERE snapshot_date < CURRENT_DATE
    )
),

-- Batch process changes to handle large datasets
batched_comparison AS (
    SELECT 
        c.*,
        p.previous_content_hash,
        p.previous_customer_name,
        p.previous_customer_segment,
        p.previous_annual_revenue,
        
        -- Efficient change detection
        CASE 
            WHEN p.customer_id IS NULL THEN 'INSERT'
            WHEN c.content_hash != p.previous_content_hash THEN 'UPDATE'
            ELSE 'NO_CHANGE'
        END as change_type,
        
        -- Specific field change detection
        ARRAY_TO_STRING(ARRAY_REMOVE(ARRAY[
            CASE WHEN c.customer_name != p.previous_customer_name THEN 'name' END,
            CASE WHEN c.customer_segment != p.previous_customer_segment THEN 'segment' END,
            CASE WHEN c.annual_revenue != p.previous_annual_revenue THEN 'revenue' END
        ], NULL), ',') as changed_fields,
        
        -- Processing batch for optimization
        FLOOR(c.row_num / 10000) as processing_batch
        
    FROM current_snapshot c
    LEFT JOIN previous_snapshot p ON c.customer_id = p.customer_id
),

-- Identify deleted customers
deleted_customers AS (
    SELECT 
        p.customer_id,
        p.previous_customer_name as customer_name,
        NULL as email,
        NULL as phone,
        NULL as address,
        p.previous_customer_segment as customer_segment,
        p.previous_annual_revenue as annual_revenue,
        'DELETED' as status,
        CURRENT_DATE as snapshot_date,
        NULL as content_hash,
        p.previous_content_hash,
        'DELETE' as change_type,
        'deleted' as changed_fields,
        0 as processing_batch,
        0 as row_num
    FROM previous_snapshot p
    LEFT JOIN current_snapshot c ON p.customer_id = c.customer_id
    WHERE c.customer_id IS NULL
)

SELECT * FROM batched_comparison WHERE change_type != 'NO_CHANGE'
UNION ALL
SELECT * FROM deleted_customers

{% else %}
-- Initial load with optimization flags
SELECT 
    customer_id,
    customer_name,
    email,
    phone,
    address,
    customer_segment,
    annual_revenue,
    status,
    CURRENT_DATE as snapshot_date,
    {{ dbt_utils.generate_surrogate_key([
        'customer_name', 'email', 'phone', 'address', 
        'customer_segment', 'annual_revenue', 'status'
    ]) }} as content_hash,
    NULL as previous_content_hash,
    NULL as previous_customer_name,
    NULL as previous_customer_segment,
    NULL as previous_annual_revenue,
    'INITIAL_LOAD' as change_type,
    'initial_load' as changed_fields,
    0 as processing_batch,
    ROW_NUMBER() OVER (ORDER BY customer_id) as row_num
FROM {{ source('crm', 'customers') }}
WHERE status != 'ARCHIVED'
{% endif %}

-- ========================================
-- Snapshot Comparison with Business Rules
-- ========================================

-- models/staging/stg_inventory_snapshot_business_rules.sql
{{
    config(
        materialized='incremental',
        unique_key='inventory_id',
        on_schema_change='sync_all_columns'
    )
}}

{% if is_incremental() %}

WITH current_inventory AS (
    SELECT 
        inventory_id,
        product_id,
        location_id,
        quantity_on_hand,
        reserved_quantity,
        reorder_point,
        max_stock_level,
        unit_cost,
        last_counted_date,
        CURRENT_DATE as snapshot_date,
        
        -- Calculate derived fields for business logic
        quantity_on_hand - reserved_quantity as available_quantity,
        CASE 
            WHEN quantity_on_hand <= reorder_point THEN 'LOW_STOCK'
            WHEN quantity_on_hand >= max_stock_level THEN 'OVERSTOCK'
            ELSE 'NORMAL'
        END as stock_status,
        
        {{ dbt_utils.generate_surrogate_key([
            'product_id', 'location_id', 'quantity_on_hand', 
            'reserved_quantity', 'reorder_point', 'max_stock_level', 'unit_cost'
        ]) }} as content_hash
        
    FROM {{ source('inventory', 'current_inventory') }}
),

previous_inventory AS (
    SELECT 
        inventory_id,
        quantity_on_hand as previous_quantity,
        available_quantity as previous_available_quantity,
        stock_status as previous_stock_status,
        unit_cost as previous_unit_cost,
        content_hash as previous_content_hash,
        snapshot_date as previous_snapshot_date
    FROM {{ this }}
    WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM {{ this }})
),

business_rule_comparison AS (
    SELECT 
        c.*,
        p.previous_quantity,
        p.previous_available_quantity,
        p.previous_stock_status,
        p.previous_unit_cost,
        p.previous_content_hash,
        
        -- Change detection with business context
        CASE 
            WHEN p.inventory_id IS NULL THEN 'NEW_INVENTORY'
            WHEN c.content_hash != p.previous_content_hash THEN 'INVENTORY_CHANGE'
            ELSE 'NO_CHANGE'
        END as change_type,
        
        -- Business-specific change categories
        CASE 
            WHEN c.quantity_on_hand != p.previous_quantity THEN
                CASE 
                    WHEN c.quantity_on_hand > p.previous_quantity THEN 'STOCK_INCREASE'
                    ELSE 'STOCK_DECREASE'
                END
        END as quantity_change_type,
        
        CASE 
            WHEN c.stock_status != p.previous_stock_status THEN
                CONCAT(p.previous_stock_status, '_TO_', c.stock_status)
        END as status_transition,
        
        -- Calculate change magnitude
        c.quantity_on_hand - COALESCE(p.previous_quantity, 0) as quantity_change,
        c.unit_cost - COALESCE(p.previous_unit_cost, 0) as unit_cost_change,
        
        -- Flag significant changes
        CASE 
            WHEN ABS(c.quantity_on_hand - COALESCE(p.previous_quantity, 0)) > 100 THEN 'LARGE_QUANTITY_CHANGE'
            WHEN c.stock_status = 'LOW_STOCK' AND p.previous_stock_status != 'LOW_STOCK' THEN 'NEW_LOW_STOCK_ALERT'
            WHEN c.stock_status = 'OVERSTOCK' AND p.previous_stock_status != 'OVERSTOCK' THEN 'NEW_OVERSTOCK_ALERT'
            WHEN ABS(c.unit_cost - COALESCE(p.previous_unit_cost, 0)) > c.unit_cost * 0.1 THEN 'SIGNIFICANT_COST_CHANGE'
            ELSE 'NORMAL_CHANGE'
        END as business_impact,
        
        p.previous_snapshot_date
        
    FROM current_inventory c
    LEFT JOIN previous_inventory p ON c.inventory_id = p.inventory_id
)

SELECT 
    *,
    CURRENT_TIMESTAMP as processed_at,
    'SNAPSHOT_COMPARISON_BUSINESS' as cdc_method
FROM business_rule_comparison
WHERE change_type != 'NO_CHANGE'
   OR business_impact != 'NORMAL_CHANGE'

{% else %}
-- Initial load
SELECT 
    inventory_id,
    product_id,
    location_id,
    quantity_on_hand,
    reserved_quantity,
    reorder_point,
    max_stock_level,
    unit_cost,
    last_counted_date,
    CURRENT_DATE as snapshot_date,
    quantity_on_hand - reserved_quantity as available_quantity,
    CASE 
        WHEN quantity_on_hand <= reorder_point THEN 'LOW_STOCK'
        WHEN quantity_on_hand >= max_stock_level THEN 'OVERSTOCK'
        ELSE 'NORMAL'
    END as stock_status,
    {{ dbt_utils.generate_surrogate_key([
        'product_id', 'location_id', 'quantity_on_hand', 
        'reserved_quantity', 'reorder_point', 'max_stock_level', 'unit_cost'
    ]) }} as content_hash,
    NULL as previous_quantity,
    NULL as previous_available_quantity,
    NULL as previous_stock_status,
    NULL as previous_unit_cost,
    NULL as previous_content_hash,
    'INITIAL_LOAD' as change_type,
    NULL as quantity_change_type,
    NULL as status_transition,
    0 as quantity_change,
    0 as unit_cost_change,
    'INITIAL_LOAD' as business_impact,
    NULL as previous_snapshot_date,
    CURRENT_TIMESTAMP as processed_at,
    'SNAPSHOT_COMPARISON_BUSINESS' as cdc_method
FROM {{ source('inventory', 'current_inventory') }}
{% endif %}

-- ========================================
-- Snapshot CDC Historical Trend Analysis
-- ========================================

-- models/analytics/supplier_change_trends.sql
WITH daily_supplier_changes AS (
    SELECT 
        snapshot_date,
        change_type,
        COUNT(*) as change_count,
        COUNT(CASE WHEN name_changed THEN 1 END) as name_changes,
        COUNT(CASE WHEN email_changed THEN 1 END) as email_changes,
        COUNT(CASE WHEN credit_rating_changed THEN 1 END) as credit_changes
    FROM {{ ref('stg_suppliers_snapshot_cdc') }}
    WHERE snapshot_date >= CURRENT_DATE - INTERVAL '90 days'
    GROUP BY snapshot_date, change_type
),

trend_analysis AS (
    SELECT 
        snapshot_date,
        change_type,
        change_count,
        
        -- Calculate moving averages
        AVG(change_count) OVER (
            PARTITION BY change_type 
            ORDER BY snapshot_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as rolling_7day_avg,
        
        -- Detect anomalies
        CASE 
            WHEN change_count > (
                AVG(change_count) OVER (
                    PARTITION BY change_type 
                    ORDER BY snapshot_date 
                    ROWS BETWEEN 29 PRECEDING AND 1 PRECEDING
                ) * 3
            ) THEN 'ANOMALY_HIGH'
            WHEN change_count = 0 AND 
                 AVG(change_count) OVER (
                    PARTITION BY change_type 
                    ORDER BY snapshot_date 
                    ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING
                 ) > 0
            THEN 'ANOMALY_LOW'
            ELSE 'NORMAL'
        END as anomaly_flag,
        
        -- Calculate change velocity
        change_count - LAG(change_count, 1, 0) OVER (
            PARTITION BY change_type 
            ORDER BY snapshot_date
        ) as day_over_day_change
        
    FROM daily_supplier_changes
)

SELECT *
FROM trend_analysis
WHERE snapshot_date >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY snapshot_date DESC, change_type;

-- ========================================
-- Snapshot Comparison Performance Monitoring
-- ========================================

-- models/monitoring/snapshot_cdc_performance.sql
WITH snapshot_processing_metrics AS (
    SELECT 
        'suppliers' as table_name,
        MAX(snapshot_date) as last_snapshot_date,
        COUNT(*) as total_records_processed,
        COUNT(CASE WHEN change_type = 'INSERT' THEN 1 END) as new_records,
        COUNT(CASE WHEN change_type = 'UPDATE' THEN 1 END) as updated_records,
        COUNT(CASE WHEN change_type = 'DELETE' THEN 1 END) as deleted_records,
        MAX(processed_at) as last_processed_at,
        
        -- Calculate processing efficiency
        COUNT(CASE WHEN change_type != 'NO_CHANGE' THEN 1 END) * 100.0 / COUNT(*) as change_rate_percent
        
    FROM {{ ref('stg_suppliers_snapshot_cdc') }}
    WHERE processed_at >= CURRENT_DATE - INTERVAL '7 days'
    
    UNION ALL
    
    SELECT 
        'inventory' as table_name,
        MAX(snapshot_date) as last_snapshot_date,
        COUNT(*) as total_records_processed,
        COUNT(CASE WHEN change_type = 'NEW_INVENTORY' THEN 1 END) as new_records,
        COUNT(CASE WHEN change_type = 'INVENTORY_CHANGE' THEN 1 END) as updated_records,
        0 as deleted_records, -- No deletes in inventory model
        MAX(processed_at) as last_processed_at,
        COUNT(CASE WHEN change_type != 'NO_CHANGE' THEN 1 END) * 100.0 / COUNT(*) as change_rate_percent
        
    FROM {{ ref('stg_inventory_snapshot_business_rules') }}
    WHERE processed_at >= CURRENT_DATE - INTERVAL '7 days'
)

SELECT 
    *,
    -- Performance indicators
    CASE 
        WHEN last_processed_at < CURRENT_TIMESTAMP - INTERVAL '25 hours' THEN 'STALE_DATA'
        WHEN change_rate_percent > 50 THEN 'HIGH_CHANGE_RATE'
        WHEN change_rate_percent = 0 THEN 'NO_CHANGES_DETECTED'
        ELSE 'NORMAL'
    END as performance_flag,
    
    EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - last_processed_at))/3600 as hours_since_last_update
    
FROM snapshot_processing_metrics;