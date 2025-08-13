-- ========================================
-- Trigger-Based CDC Implementation
-- ========================================

-- Processing Database Audit Table Data
-- models/staging/stg_product_trigger_cdc.sql
{{
    config(
        materialized='incremental',
        unique_key='audit_id',
        on_schema_change='sync_all_columns'
    )
}}

WITH audit_table_changes AS (
    SELECT 
        audit_id,
        table_name,
        operation_type, -- I (INSERT), U (UPDATE), D (DELETE)
        primary_key_value,
        column_name,
        old_value,
        new_value,
        changed_by,
        changed_at,
        session_id,
        application_name,
        
        -- Parse primary key
        primary_key_value::INTEGER as product_id
        
    FROM {{ source('audit', 'audit_log') }}
    WHERE table_name = 'products'
    
    {% if is_incremental() %}
        AND changed_at > (
            SELECT COALESCE(MAX(changed_at), '1900-01-01'::timestamp)
            FROM {{ this }}
        )
    {% endif %}
),

-- Pivot audit columns back to row format
pivoted_changes AS (
    SELECT 
        audit_id,
        product_id,
        operation_type,
        changed_by,
        changed_at,
        session_id,
        application_name,
        
        -- Reconstruct the record from pivoted audit data
        MAX(CASE WHEN column_name = 'product_name' THEN new_value END) as product_name,
        MAX(CASE WHEN column_name = 'category_id' THEN new_value::INTEGER END) as category_id,
        MAX(CASE WHEN column_name = 'price' THEN new_value::DECIMAL(10,2) END) as price,
        MAX(CASE WHEN column_name = 'status' THEN new_value END) as status,
        MAX(CASE WHEN column_name = 'description' THEN new_value END) as description,
        
        -- Capture previous values for UPDATE operations
        MAX(CASE WHEN column_name = 'product_name' AND operation_type = 'U' 
                THEN old_value END) as previous_product_name,
        MAX(CASE WHEN column_name = 'price' AND operation_type = 'U' 
                THEN old_value::DECIMAL(10,2) END) as previous_price,
        MAX(CASE WHEN column_name = 'status' AND operation_type = 'U' 
                THEN old_value END) as previous_status,
        
        -- Track which columns changed
        LISTAGG(
            CASE WHEN old_value != new_value OR old_value IS NULL OR new_value IS NULL 
                 THEN column_name 
            END, ', '
        ) WITHIN GROUP (ORDER BY column_name) as changed_columns,
        
        COUNT(*) as columns_changed
        
    FROM audit_table_changes
    GROUP BY 
        audit_id, product_id, operation_type, changed_by, 
        changed_at, session_id, application_name
)

SELECT 
    *,
    -- Add CDC processing metadata
    CURRENT_TIMESTAMP as processed_at,
    'TRIGGER_BASED' as cdc_method,
    
    -- Standardize operation types
    CASE operation_type
        WHEN 'I' THEN 'INSERT'
        WHEN 'U' THEN 'UPDATE'  
        WHEN 'D' THEN 'DELETE'
    END as standardized_operation
FROM pivoted_changes;

-- ========================================
-- Trigger-Based CDC with Change Categorization
-- ========================================

-- models/staging/stg_employee_trigger_cdc_categorized.sql
{{
    config(
        materialized='incremental',
        unique_key='audit_id',
        on_schema_change='sync_all_columns'
    )
}}

WITH raw_audit_data AS (
    SELECT 
        audit_id,
        table_name,
        operation_type,
        primary_key_value::INTEGER as employee_id,
        
        -- Extract change details from JSON or structured columns
        CASE 
            WHEN change_data IS NOT NULL THEN change_data
            ELSE JSON_BUILD_OBJECT(
                'old_values', old_values,
                'new_values', new_values,
                'changed_columns', changed_columns
            )
        END as change_payload,
        
        changed_by,
        changed_at,
        client_ip,
        application_name,
        transaction_id
        
    FROM {{ source('audit', 'employee_audit') }}
    
    {% if is_incremental() %}
        AND changed_at > (
            SELECT COALESCE(MAX(changed_at), '1900-01-01'::timestamp)
            FROM {{ this }}
        )
    {% endif %}
),

categorized_changes AS (
    SELECT 
        audit_id,
        employee_id,
        operation_type,
        
        -- Extract current values
        JSON_EXTRACT_SCALAR(change_payload, '$.new_values.first_name') as first_name,
        JSON_EXTRACT_SCALAR(change_payload, '$.new_values.last_name') as last_name,
        JSON_EXTRACT_SCALAR(change_payload, '$.new_values.email') as email,
        JSON_EXTRACT_SCALAR(change_payload, '$.new_values.department') as department,
        JSON_EXTRACT_SCALAR(change_payload, '$.new_values.salary')::DECIMAL(10,2) as salary,
        JSON_EXTRACT_SCALAR(change_payload, '$.new_values.status') as status,
        
        -- Extract previous values
        JSON_EXTRACT_SCALAR(change_payload, '$.old_values.department') as previous_department,
        JSON_EXTRACT_SCALAR(change_payload, '$.old_values.salary')::DECIMAL(10,2) as previous_salary,
        JSON_EXTRACT_SCALAR(change_payload, '$.old_values.status') as previous_status,
        
        -- Categorize the type of change
        CASE 
            WHEN operation_type = 'I' THEN 'NEW_HIRE'
            WHEN operation_type = 'D' THEN 'TERMINATION'
            WHEN JSON_EXTRACT_SCALAR(change_payload, '$.old_values.department') != 
                 JSON_EXTRACT_SCALAR(change_payload, '$.new_values.department') 
                 THEN 'DEPARTMENT_TRANSFER'
            WHEN JSON_EXTRACT_SCALAR(change_payload, '$.old_values.salary')::DECIMAL(10,2) != 
                 JSON_EXTRACT_SCALAR(change_payload, '$.new_values.salary')::DECIMAL(10,2)
                 THEN 'SALARY_CHANGE'
            WHEN JSON_EXTRACT_SCALAR(change_payload, '$.old_values.status') != 
                 JSON_EXTRACT_SCALAR(change_payload, '$.new_values.status')
                 THEN 'STATUS_CHANGE'
            ELSE 'PROFILE_UPDATE'
        END as change_category,
        
        -- Calculate salary change details
        CASE 
            WHEN operation_type = 'U' AND 
                 JSON_EXTRACT_SCALAR(change_payload, '$.old_values.salary') IS NOT NULL
            THEN JSON_EXTRACT_SCALAR(change_payload, '$.new_values.salary')::DECIMAL(10,2) - 
                 JSON_EXTRACT_SCALAR(change_payload, '$.old_values.salary')::DECIMAL(10,2)
            ELSE NULL
        END as salary_change_amount,
        
        -- Audit metadata
        changed_by,
        changed_at,
        client_ip,
        application_name,
        transaction_id,
        change_payload
        
    FROM raw_audit_data
),

enriched_changes AS (
    SELECT 
        *,
        -- Add business logic for change validation
        CASE 
            WHEN change_category = 'SALARY_CHANGE' AND ABS(salary_change_amount) > 10000
            THEN 'HIGH_VALUE_SALARY_CHANGE'
            WHEN change_category = 'DEPARTMENT_TRANSFER' AND 
                 previous_department IN ('Finance', 'Executive') AND
                 department NOT IN ('Finance', 'Executive')
            THEN 'SENSITIVE_DEPARTMENT_EXIT'
            WHEN changed_by != 'SYSTEM' AND 
                 EXTRACT(DOW FROM changed_at) IN (0, 6) -- Weekend
            THEN 'WEEKEND_MANUAL_CHANGE'
            ELSE 'NORMAL_CHANGE'
        END as risk_category,
        
        -- Calculate change frequency
        COUNT(*) OVER (
            PARTITION BY employee_id 
            ORDER BY changed_at 
            RANGE BETWEEN INTERVAL '7 days' PRECEDING AND CURRENT ROW
        ) as changes_in_last_week,
        
        CURRENT_TIMESTAMP as processed_at
        
    FROM categorized_changes
)

SELECT *
FROM enriched_changes;

-- ========================================
-- Trigger-Based CDC Current State Reconstruction
-- ========================================

-- models/marts/dim_employees_current.sql
{{
    config(
        materialized='incremental',
        unique_key='employee_id',
        incremental_strategy='merge'
    )
}}

WITH latest_employee_changes AS (
    SELECT 
        employee_id,
        first_name,
        last_name,
        email,
        department,
        salary,
        status,
        operation_type,
        changed_at,
        changed_by,
        
        -- Get the most recent change per employee
        ROW_NUMBER() OVER (
            PARTITION BY employee_id 
            ORDER BY changed_at DESC, audit_id DESC
        ) = 1 as is_latest_change
        
    FROM {{ ref('stg_employee_trigger_cdc_categorized') }}
    
    {% if is_incremental() %}
        WHERE employee_id IN (
            SELECT DISTINCT employee_id
            FROM {{ ref('stg_employee_trigger_cdc_categorized') }}
            WHERE processed_at > (
                SELECT COALESCE(MAX(last_updated_at), '1900-01-01'::timestamp)
                FROM {{ this }}
            )
        )
    {% endif %}
),

current_employee_state AS (
    SELECT 
        employee_id,
        first_name,
        last_name,
        email,
        department,
        salary,
        status,
        changed_at as last_updated_at,
        changed_by as last_updated_by,
        
        -- Employee lifecycle status
        CASE 
            WHEN operation_type = 'D' THEN TRUE
            WHEN status IN ('TERMINATED', 'INACTIVE') THEN TRUE
            ELSE FALSE
        END as is_inactive,
        
        -- Derived fields
        CONCAT(first_name, ' ', last_name) as full_name,
        
        -- Audit metadata
        operation_type as last_operation,
        CURRENT_TIMESTAMP as record_updated_at
        
    FROM latest_employee_changes
    WHERE is_latest_change = TRUE
)

SELECT *
FROM current_employee_state
-- Include inactive employees for historical reporting
-- WHERE NOT is_inactive; -- Uncomment to exclude inactive employees

-- ========================================
-- Trigger-Based CDC Data Quality Monitoring
-- ========================================

-- models/monitoring/trigger_cdc_data_quality.sql
WITH audit_quality_metrics AS (
    SELECT 
        DATE(changed_at) as audit_date,
        table_name,
        operation_type,
        
        -- Volume metrics
        COUNT(*) as total_changes,
        COUNT(DISTINCT primary_key_value) as unique_records_changed,
        COUNT(DISTINCT changed_by) as unique_users,
        COUNT(DISTINCT session_id) as unique_sessions,
        
        -- Timing metrics
        MIN(changed_at) as first_change_time,
        MAX(changed_at) as last_change_time,
        
        -- Quality indicators
        SUM(CASE WHEN changed_by = 'SYSTEM' THEN 1 ELSE 0 END) as system_changes,
        SUM(CASE WHEN changed_by != 'SYSTEM' THEN 1 ELSE 0 END) as manual_changes,
        
        -- Anomaly detection
        AVG(columns_changed) as avg_columns_per_change,
        MAX(columns_changed) as max_columns_per_change,
        
        -- Change patterns
        SUM(CASE WHEN operation_type = 'I' THEN 1 ELSE 0 END) as inserts,
        SUM(CASE WHEN operation_type = 'U' THEN 1 ELSE 0 END) as updates,
        SUM(CASE WHEN operation_type = 'D' THEN 1 ELSE 0 END) as deletes
        
    FROM {{ ref('stg_product_trigger_cdc') }}
    WHERE changed_at >= CURRENT_DATE - INTERVAL '7 days'
    GROUP BY DATE(changed_at), table_name, operation_type
),

quality_alerts AS (
    SELECT 
        *,
        -- Alert conditions
        CASE 
            WHEN total_changes > (
                SELECT AVG(total_changes) * 3 
                FROM audit_quality_metrics 
                WHERE audit_date >= CURRENT_DATE - INTERVAL '30 days'
            ) THEN 'HIGH_VOLUME_ALERT'
            WHEN manual_changes > total_changes * 0.8 THEN 'HIGH_MANUAL_CHANGE_ALERT'
            WHEN deletes > inserts * 2 THEN 'HIGH_DELETE_RATIO_ALERT'
            ELSE 'NORMAL'
        END as quality_alert_level
        
    FROM audit_quality_metrics
)

SELECT *
FROM quality_alerts
WHERE quality_alert_level != 'NORMAL'
   OR audit_date = CURRENT_DATE;

-- ========================================
-- Trigger-Based CDC Historical Analysis
-- ========================================

-- models/analytics/employee_change_patterns.sql
WITH employee_change_timeline AS (
    SELECT 
        employee_id,
        changed_at,
        change_category,
        risk_category,
        salary,
        previous_salary,
        department,
        previous_department,
        changed_by,
        
        -- Calculate time between changes
        LAG(changed_at) OVER (
            PARTITION BY employee_id 
            ORDER BY changed_at
        ) as previous_change_time,
        
        -- Track change sequence
        ROW_NUMBER() OVER (
            PARTITION BY employee_id 
            ORDER BY changed_at
        ) as change_sequence_number
        
    FROM {{ ref('stg_employee_trigger_cdc_categorized') }}
    WHERE changed_at >= CURRENT_DATE - INTERVAL '2 years'
),

change_pattern_analysis AS (
    SELECT 
        employee_id,
        
        -- Change frequency metrics
        COUNT(*) as total_changes,
        COUNT(DISTINCT change_category) as change_types,
        AVG(EXTRACT(EPOCH FROM (changed_at - previous_change_time))/86400) as avg_days_between_changes,
        
        -- Career progression indicators
        COUNT(CASE WHEN change_category = 'DEPARTMENT_TRANSFER' THEN 1 END) as department_changes,
        COUNT(CASE WHEN change_category = 'SALARY_CHANGE' AND salary > previous_salary THEN 1 END) as salary_increases,
        COUNT(CASE WHEN change_category = 'SALARY_CHANGE' AND salary < previous_salary THEN 1 END) as salary_decreases,
        
        -- Risk indicators
        COUNT(CASE WHEN risk_category != 'NORMAL_CHANGE' THEN 1 END) as high_risk_changes,
        MAX(CASE WHEN risk_category = 'HIGH_VALUE_SALARY_CHANGE' THEN salary END) as highest_salary,
        
        -- Timeline
        MIN(changed_at) as first_change_date,
        MAX(changed_at) as last_change_date,
        MAX(change_sequence_number) as total_change_events
        
    FROM employee_change_timeline
    GROUP BY employee_id
)

SELECT 
    *,
    -- Classification
    CASE 
        WHEN total_changes >= 10 THEN 'HIGH_ACTIVITY'
        WHEN total_changes >= 5 THEN 'MEDIUM_ACTIVITY'
        ELSE 'LOW_ACTIVITY'
    END as activity_level,
    
    CASE 
        WHEN department_changes >= 3 THEN 'HIGH_MOBILITY'
        WHEN department_changes >= 1 THEN 'SOME_MOBILITY'
        ELSE 'STABLE'
    END as mobility_pattern
    
FROM change_pattern_analysis;