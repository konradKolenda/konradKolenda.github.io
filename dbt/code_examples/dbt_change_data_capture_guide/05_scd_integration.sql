-- ========================================
-- SCD Type 2 Integration with CDC
-- ========================================

-- SCD Type 2 Customer Dimension with CDC
-- models/marts/dim_customers_scd2_cdc.sql
{{
    config(
        materialized='incremental',
        unique_key='surrogate_key',
        on_schema_change='sync_all_columns'
    )
}}

WITH cdc_changes AS (
    SELECT 
        customer_id,
        customer_name,
        email,
        phone,
        address,
        customer_segment,
        annual_revenue,
        status,
        updated_at,
        cdc_operation_type,
        processed_at
    FROM {{ ref('stg_customers_timestamp_cdc') }}
    
    {% if is_incremental() %}
        WHERE processed_at > (
            SELECT COALESCE(MAX(processed_at), '1900-01-01'::timestamp)
            FROM {{ this }}
        )
    {% endif %}
),

-- Get currently active records for comparison
{% if is_incremental() %}
current_active_records AS (
    SELECT 
        customer_id,
        customer_name,
        email,
        phone,
        address,
        customer_segment,
        annual_revenue,
        status,
        surrogate_key,
        effective_start_date,
        effective_end_date,
        is_current
    FROM {{ this }}
    WHERE is_current = TRUE
      AND customer_id IN (
          SELECT DISTINCT customer_id FROM cdc_changes
      )
),
{% endif %}

-- Create new SCD Type 2 records for changes
new_scd_records AS (
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['customer_id', 'updated_at']) }} as surrogate_key,
        customer_id,
        customer_name,
        email,
        phone,
        address,
        customer_segment,
        annual_revenue,
        status,
        updated_at as effective_start_date,
        NULL as effective_end_date,
        TRUE as is_current,
        cdc_operation_type,
        processed_at,
        'NEW_VERSION' as scd_action
    FROM cdc_changes
    WHERE cdc_operation_type != 'DELETE'
),

-- Expire current records that have been updated or deleted
{% if is_incremental() %}
expired_records AS (
    SELECT 
        c.surrogate_key,
        c.customer_id,
        c.customer_name,
        c.email,
        c.phone,
        c.address,
        c.customer_segment,
        c.annual_revenue,
        c.status,
        c.effective_start_date,
        cc.updated_at as effective_end_date, -- Close the record
        FALSE as is_current,
        c.cdc_operation_type,
        c.processed_at,
        'EXPIRED' as scd_action
    FROM current_active_records c
    INNER JOIN cdc_changes cc ON c.customer_id = cc.customer_id
    WHERE cc.cdc_operation_type IN ('UPDATE', 'DELETE')
),
{% endif %}

-- Handle deleted customers (close record without creating new version)
deleted_customer_records AS (
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['customer_id', 'updated_at', 'DELETE']) }} as surrogate_key,
        customer_id,
        customer_name,
        email,
        phone,
        address,
        customer_segment,
        annual_revenue,
        'DELETED' as status,
        updated_at as effective_start_date,
        updated_at as effective_end_date, -- Immediately expired
        FALSE as is_current,
        cdc_operation_type,
        processed_at,
        'DELETED' as scd_action
    FROM cdc_changes
    WHERE cdc_operation_type = 'DELETE'
),

-- Combine all SCD operations
all_scd_changes AS (
    SELECT * FROM new_scd_records
    
    {% if is_incremental() %}
    UNION ALL
    SELECT * FROM expired_records
    {% endif %}
    
    UNION ALL
    SELECT * FROM deleted_customer_records
)

SELECT 
    *,
    -- Add SCD metadata
    CASE 
        WHEN effective_end_date IS NULL THEN CURRENT_TIMESTAMP + INTERVAL '100 years'
        ELSE effective_end_date
    END as effective_end_date_calculated,
    
    CURRENT_TIMESTAMP as record_created_at,
    
    -- Version number within customer
    ROW_NUMBER() OVER (
        PARTITION BY customer_id 
        ORDER BY effective_start_date
    ) as version_number
    
FROM all_scd_changes;

-- ========================================
-- SCD Type 1 Integration with CDC
-- ========================================

-- models/marts/dim_products_scd1_cdc.sql
{{
    config(
        materialized='incremental',
        unique_key='product_id',
        incremental_strategy='merge'
    )
}}

WITH latest_product_changes AS (
    SELECT 
        product_id,
        product_name,
        category_id,
        price,
        status,
        updated_at,
        cdc_operation_type,
        processed_at,
        
        -- Get only the latest change per product
        ROW_NUMBER() OVER (
            PARTITION BY product_id 
            ORDER BY updated_at DESC
        ) = 1 as is_latest_change
        
    FROM {{ ref('stg_products_advanced_timestamp_cdc') }}
    
    {% if is_incremental() %}
        WHERE processed_at > (
            SELECT COALESCE(MAX(last_updated_at), '1900-01-01'::timestamp)
            FROM {{ this }}
        )
    {% endif %}
),

scd1_updates AS (
    SELECT 
        product_id,
        product_name,
        category_id,
        price,
        CASE 
            WHEN cdc_operation_type = 'DELETE' THEN 'DELETED'
            ELSE status
        END as status,
        
        -- SCD Type 1 metadata
        updated_at as last_updated_at,
        cdc_operation_type as last_operation,
        processed_at as last_processed_at,
        
        -- Track change history in JSON for audit
        JSON_BUILD_OBJECT(
            'operation', cdc_operation_type,
            'timestamp', updated_at,
            'processed_at', processed_at
        ) as last_change_details,
        
        CURRENT_TIMESTAMP as dimension_updated_at
        
    FROM latest_product_changes
    WHERE is_latest_change = TRUE
      AND cdc_operation_type != 'DELETE' -- Exclude hard deletes for SCD1
)

SELECT *
FROM scd1_updates;

-- ========================================
-- SCD Type 3 Integration with CDC
-- ========================================

-- models/marts/dim_suppliers_scd3_cdc.sql
{{
    config(
        materialized='incremental',
        unique_key='supplier_id',
        incremental_strategy='merge'
    )
}}

WITH supplier_changes AS (
    SELECT 
        supplier_id,
        supplier_name,
        contact_email,
        credit_rating,
        status,
        updated_at,
        change_type,
        processed_at,
        
        -- Track if specific fields changed
        name_changed,
        email_changed,
        credit_rating_changed,
        
        -- Previous values from snapshot comparison
        previous_supplier_name,
        previous_contact_email,
        previous_credit_rating
        
    FROM {{ ref('stg_suppliers_snapshot_cdc') }}
    
    {% if is_incremental() %}
        WHERE processed_at > (
            SELECT COALESCE(MAX(last_updated_at), '1900-01-01'::timestamp)
            FROM {{ this }}
        )
    {% endif %}
),

-- Get current dimension state for merging
{% if is_incremental() %}
current_dimension AS (
    SELECT 
        supplier_id,
        supplier_name,
        previous_supplier_name as current_previous_name,
        contact_email,
        previous_contact_email as current_previous_email,
        credit_rating,
        previous_credit_rating as current_previous_rating,
        last_updated_at
    FROM {{ this }}
),
{% endif %}

scd3_processing AS (
    SELECT 
        sc.supplier_id,
        
        -- Current values
        sc.supplier_name,
        sc.contact_email,
        sc.credit_rating,
        sc.status,
        
        -- SCD Type 3: Track one previous version
        CASE 
            WHEN sc.name_changed THEN sc.previous_supplier_name
            {% if is_incremental() %}
            ELSE COALESCE(cd.current_previous_name, sc.supplier_name)
            {% else %}
            ELSE NULL
            {% endif %}
        END as previous_supplier_name,
        
        CASE 
            WHEN sc.email_changed THEN sc.previous_contact_email
            {% if is_incremental() %}
            ELSE COALESCE(cd.current_previous_email, sc.contact_email)
            {% else %}
            ELSE NULL
            {% endif %}
        END as previous_contact_email,
        
        CASE 
            WHEN sc.credit_rating_changed THEN sc.previous_credit_rating
            {% if is_incremental() %}
            ELSE COALESCE(cd.current_previous_rating, sc.credit_rating)
            {% else %}
            ELSE NULL
            {% endif %}
        END as previous_credit_rating,
        
        -- Change tracking
        sc.name_changed,
        sc.email_changed, 
        sc.credit_rating_changed,
        
        -- Timestamps
        sc.updated_at as last_updated_at,
        CASE 
            WHEN sc.name_changed OR sc.email_changed OR sc.credit_rating_changed 
            THEN sc.updated_at
            {% if is_incremental() %}
            ELSE cd.last_updated_at
            {% else %}
            ELSE NULL
            {% endif %}
        END as last_change_date,
        
        sc.processed_at,
        CURRENT_TIMESTAMP as dimension_updated_at
        
    FROM supplier_changes sc
    {% if is_incremental() %}
    LEFT JOIN current_dimension cd ON sc.supplier_id = cd.supplier_id
    {% endif %}
    WHERE sc.change_type != 'NO_CHANGE'
)

SELECT *
FROM scd3_processing;

-- ========================================
-- Mixed SCD Strategy with CDC
-- ========================================

-- models/marts/dim_employees_mixed_scd.sql
{{
    config(
        materialized='incremental',
        unique_key='surrogate_key',
        on_schema_change='sync_all_columns'
    )
}}

WITH employee_changes AS (
    SELECT 
        employee_id,
        first_name,
        last_name,
        email,
        department,
        salary,
        status,
        change_category,
        risk_category,
        changed_at,
        processed_at
    FROM {{ ref('stg_employee_trigger_cdc_categorized') }}
    
    {% if is_incremental() %}
        WHERE processed_at > (
            SELECT COALESCE(MAX(processed_at), '1900-01-01'::timestamp)
            FROM {{ this }}
        )
    {% endif %}
),

-- Apply different SCD strategies based on change type
scd_strategy_assignment AS (
    SELECT 
        *,
        -- Assign SCD strategy based on business rules
        CASE 
            WHEN change_category IN ('DEPARTMENT_TRANSFER', 'SALARY_CHANGE') THEN 'SCD2'
            WHEN change_category IN ('NEW_HIRE', 'TERMINATION') THEN 'SCD2'
            ELSE 'SCD1'
        END as scd_strategy,
        
        -- Determine if this change should create a new version
        CASE 
            WHEN change_category IN ('DEPARTMENT_TRANSFER', 'SALARY_CHANGE', 'NEW_HIRE') THEN TRUE
            ELSE FALSE
        END as creates_new_version
        
    FROM employee_changes
),

-- Process SCD Type 1 changes (profile updates)
scd1_changes AS (
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['employee_id']) }} as surrogate_key,
        employee_id,
        first_name,
        last_name,
        email,
        department,
        salary,
        status,
        changed_at as effective_start_date,
        NULL as effective_end_date,
        TRUE as is_current,
        change_category,
        risk_category,
        changed_at,
        processed_at,
        'SCD1_UPDATE' as scd_action
        
    FROM scd_strategy_assignment
    WHERE scd_strategy = 'SCD1'
),

-- Process SCD Type 2 changes (department/salary changes)
scd2_new_versions AS (
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['employee_id', 'changed_at']) }} as surrogate_key,
        employee_id,
        first_name,
        last_name,
        email,
        department,
        salary,
        status,
        changed_at as effective_start_date,
        NULL as effective_end_date,
        TRUE as is_current,
        change_category,
        risk_category,
        changed_at,
        processed_at,
        'SCD2_NEW_VERSION' as scd_action
        
    FROM scd_strategy_assignment
    WHERE scd_strategy = 'SCD2'
      AND creates_new_version = TRUE
),

-- Close previous SCD Type 2 records
{% if is_incremental() %}
scd2_expired_records AS (
    SELECT 
        d.surrogate_key,
        d.employee_id,
        d.first_name,
        d.last_name,
        d.email,
        d.department,
        d.salary,
        d.status,
        d.effective_start_date,
        c.changed_at as effective_end_date, -- Close the record
        FALSE as is_current,
        d.change_category,
        d.risk_category,
        d.changed_at,
        d.processed_at,
        'SCD2_EXPIRED' as scd_action
        
    FROM {{ this }} d
    INNER JOIN scd_strategy_assignment c ON d.employee_id = c.employee_id
    WHERE d.is_current = TRUE
      AND c.scd_strategy = 'SCD2'
      AND c.creates_new_version = TRUE
),
{% endif %}

-- Combine all SCD operations
final_scd_records AS (
    SELECT * FROM scd1_changes
    UNION ALL
    SELECT * FROM scd2_new_versions
    
    {% if is_incremental() %}
    UNION ALL
    SELECT * FROM scd2_expired_records
    {% endif %}
)

SELECT 
    *,
    -- Add derived attributes
    CONCAT(first_name, ' ', last_name) as full_name,
    
    -- Calculate tenure at current position
    CASE 
        WHEN scd_action IN ('SCD2_NEW_VERSION', 'SCD1_UPDATE') THEN
            EXTRACT(DAYS FROM (CURRENT_DATE - effective_start_date))
        ELSE NULL
    END as days_in_current_position,
    
    CURRENT_TIMESTAMP as record_created_at
    
FROM final_scd_records;

-- ========================================
-- SCD Performance Optimization
-- ========================================

-- models/staging/stg_scd_processing_optimized.sql
{{
    config(
        materialized='incremental',
        unique_key='surrogate_key',
        partition_by={
            'field': 'effective_start_date',
            'data_type': 'date',
            'granularity': 'month'
        },
        cluster_by=['customer_id', 'is_current']
    )
}}

WITH optimized_cdc_input AS (
    SELECT 
        customer_id,
        customer_name,
        email,
        address,
        customer_segment,
        updated_at,
        cdc_operation_type,
        
        -- Pre-calculate hash for efficient change detection
        {{ dbt_utils.generate_surrogate_key([
            'customer_name', 'email', 'address', 'customer_segment'
        ]) }} as content_hash,
        
        processed_at
        
    FROM {{ ref('stg_customers_timestamp_cdc') }}
    
    {% if is_incremental() %}
        WHERE processed_at > (
            SELECT COALESCE(MAX(processed_at), '1900-01-01'::timestamp)
            FROM {{ this }}
        )
    {% endif %}
),

-- Batch processing for large datasets
batched_scd_processing AS (
    SELECT 
        *,
        -- Create processing batches
        NTILE({{ var('scd_processing_batches', 10) }}) OVER (
            ORDER BY customer_id
        ) as processing_batch,
        
        -- Efficient deduplication
        ROW_NUMBER() OVER (
            PARTITION BY customer_id 
            ORDER BY updated_at DESC
        ) = 1 as is_latest_change_per_customer
        
    FROM optimized_cdc_input
),

-- Create SCD records with optimized logic
scd_records AS (
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['customer_id', 'updated_at']) }} as surrogate_key,
        customer_id,
        customer_name,
        email,
        address,
        customer_segment,
        content_hash,
        updated_at as effective_start_date,
        NULL as effective_end_date,
        TRUE as is_current,
        cdc_operation_type,
        processed_at,
        processing_batch
        
    FROM batched_scd_processing
    WHERE is_latest_change_per_customer = TRUE
      AND cdc_operation_type != 'DELETE'
)

SELECT *
FROM scd_records;

-- ========================================
-- SCD Data Quality and Validation
-- ========================================

-- models/monitoring/scd_data_quality_checks.sql
WITH scd_validation AS (
    SELECT 
        'dim_customers_scd2_cdc' as table_name,
        customer_id,
        COUNT(*) as total_versions,
        SUM(CASE WHEN is_current THEN 1 ELSE 0 END) as current_versions,
        MIN(effective_start_date) as first_effective_date,
        MAX(effective_start_date) as last_effective_date,
        
        -- Data quality checks
        COUNT(CASE WHEN effective_start_date > effective_end_date THEN 1 END) as invalid_date_ranges,
        COUNT(CASE WHEN effective_end_date IS NULL AND NOT is_current THEN 1 END) as unclosed_inactive_records,
        COUNT(CASE WHEN is_current AND effective_end_date IS NOT NULL THEN 1 END) as current_with_end_date
        
    FROM {{ ref('dim_customers_scd2_cdc') }}
    GROUP BY customer_id
),

quality_summary AS (
    SELECT 
        table_name,
        COUNT(*) as total_customers,
        AVG(total_versions) as avg_versions_per_customer,
        MAX(total_versions) as max_versions_per_customer,
        SUM(invalid_date_ranges) as total_invalid_date_ranges,
        SUM(unclosed_inactive_records) as total_unclosed_inactive_records,
        SUM(current_with_end_date) as total_current_with_end_date,
        
        -- Quality indicators
        CASE 
            WHEN SUM(invalid_date_ranges) > 0 THEN 'DATE_RANGE_ISSUES'
            WHEN SUM(unclosed_inactive_records) > 0 THEN 'UNCLOSED_RECORDS'
            WHEN SUM(current_with_end_date) > 0 THEN 'CURRENT_RECORD_ISSUES'
            ELSE 'PASSED'
        END as quality_status
        
    FROM scd_validation
    GROUP BY table_name
),

-- Check for overlapping records (should not exist in proper SCD Type 2)
overlap_check AS (
    SELECT 
        'overlap_check' as check_type,
        customer_id,
        COUNT(*) as overlapping_records
    FROM {{ ref('dim_customers_scd2_cdc') }} d1
    WHERE EXISTS (
        SELECT 1 
        FROM {{ ref('dim_customers_scd2_cdc') }} d2
        WHERE d1.customer_id = d2.customer_id
          AND d1.surrogate_key != d2.surrogate_key
          AND d1.effective_start_date <= COALESCE(d2.effective_end_date, '9999-12-31')
          AND COALESCE(d1.effective_end_date, '9999-12-31') >= d2.effective_start_date
    )
    GROUP BY customer_id
    HAVING COUNT(*) > 0
)

SELECT * FROM quality_summary
UNION ALL
SELECT 
    'OVERLAP_ISSUES' as table_name,
    COUNT(*) as total_customers,
    NULL as avg_versions_per_customer,
    NULL as max_versions_per_customer,
    NULL as total_invalid_date_ranges,
    NULL as total_unclosed_inactive_records,
    NULL as total_current_with_end_date,
    CASE WHEN COUNT(*) > 0 THEN 'OVERLAP_DETECTED' ELSE 'PASSED' END as quality_status
FROM overlap_check;