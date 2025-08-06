-- ================================================================
-- Migration Strategy: Phased Approach from SQL Server to dbt
-- ================================================================

-- =====================================
-- PHASE 1: ASSESSMENT & DISCOVERY
-- =====================================

-- Step 1A: Inventory existing constraints (Run in SQL Server)
/*
SELECT 
    t.TABLE_SCHEMA,
    t.TABLE_NAME,
    c.COLUMN_NAME,
    c.DATA_TYPE,
    c.IS_NULLABLE,
    tc.CONSTRAINT_TYPE,
    tc.CONSTRAINT_NAME
FROM INFORMATION_SCHEMA.TABLES t
LEFT JOIN INFORMATION_SCHEMA.COLUMNS c 
    ON t.TABLE_NAME = c.TABLE_NAME 
    AND t.TABLE_SCHEMA = c.TABLE_SCHEMA
LEFT JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ccu 
    ON c.TABLE_NAME = ccu.TABLE_NAME 
    AND c.COLUMN_NAME = ccu.COLUMN_NAME
LEFT JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc 
    ON ccu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
WHERE t.TABLE_TYPE = 'BASE TABLE'
ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME, c.ORDINAL_POSITION;
*/

-- Step 1B: Document business rules and complex constraints
-- Create mapping document for constraint migration

-- =====================================
-- PHASE 2: INITIAL DBT SETUP
-- =====================================

-- Step 2A: Create source definitions
-- File: models/sources.yml
/*
version: 2
sources:
  - name: sqlserver_raw
    description: "Data extracted from SQL Server"
    tables:
      - name: customers
        description: "Customer master from CRM"
        columns:
          - name: customer_id
            description: "Primary key"
          - name: email
            description: "Email address (unique)"
*/

-- Step 2B: Create staging models with basic transformations
-- File: models/staging/stg_customers.sql
-- =====================================
{{ config(materialized='table') }}

SELECT 
    customer_id,
    -- Data cleaning and standardization
    LOWER(TRIM(email)) as email,
    TRIM(first_name) as first_name,
    TRIM(last_name) as last_name,
    CASE 
        WHEN phone = '' THEN NULL 
        ELSE TRIM(phone) 
    END as phone,
    created_at,
    COALESCE(NULLIF(status, ''), 'active') as status,
    -- Audit columns
    CURRENT_TIMESTAMP as _loaded_at
FROM {{ source('sqlserver_raw', 'customers') }}

-- =====================================
-- PHASE 3: IMPLEMENT CORE DATA TESTS
-- =====================================

-- Step 3A: Start with essential tests (Primary Keys, Foreign Keys)
-- File: models/staging/schema.yml (Initial Version)
/*
version: 2
models:
  - name: stg_customers
    columns:
      - name: customer_id
        data_tests:
          - unique
          - not_null
      - name: email
        data_tests:
          - unique
          - not_null
*/

-- Step 3B: Add referential integrity tests
-- Progressive rollout with warnings first
/*
  - name: stg_orders
    columns:
      - name: customer_id
        data_tests:
          - relationships:
              to: ref('stg_customers')
              field: customer_id
              config:
                severity: warn  # Start with warnings
*/

-- =====================================
-- PHASE 4: ADD BUSINESS RULE VALIDATION
-- =====================================

-- Step 4A: Convert CHECK constraints to data tests
-- Traditional CHECK constraint:
-- CHECK (total_amount > 0)

-- dbt equivalent test:
-- File: tests/assert_positive_amounts.sql
{{ config(severity='warn') }}  -- Start lenient, then make stricter

SELECT 
    order_id,
    total_amount,
    'Order total must be positive' as error_message
FROM {{ ref('stg_orders') }}
WHERE total_amount <= 0

-- Step 4B: Implement accepted_values tests
-- Traditional: status IN ('active', 'inactive', 'suspended')
-- dbt: accepted_values test in schema.yml

-- =====================================
-- PHASE 5: ADVANCED VALIDATION PATTERNS
-- =====================================

-- Step 5A: Cross-table validation
-- File: tests/assert_customer_order_integrity.sql
{{ config(severity='error') }}

-- Ensure orders reference valid customers
SELECT 
    o.order_id,
    o.customer_id,
    'Order references non-existent customer' as error_message
FROM {{ ref('stg_orders') }} o
LEFT JOIN {{ ref('stg_customers') }} c ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL

-- Step 5B: Data quality monitoring
-- File: tests/data_quality/email_format_validation.sql
{{ config(
    severity='warn',
    store_failures=true,
    store_failures_as='email_validation_failures'
) }}

SELECT 
    customer_id,
    email,
    'Invalid email format detected' as data_quality_issue,
    CURRENT_TIMESTAMP as detected_at
FROM {{ ref('stg_customers') }}
WHERE email NOT REGEXP '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'

-- =====================================
-- PHASE 6: HYBRID APPROACH (TRANSITION)
-- =====================================

-- For critical tables, maintain some database constraints during transition
-- Raw layer with minimal constraints
CREATE TABLE raw_customers_minimal (
    customer_id INT NOT NULL,  -- Keep essential constraints
    email VARCHAR(255),        -- Remove uniqueness during transition
    first_name VARCHAR(100),   -- Allow nulls during data cleaning
    last_name VARCHAR(100),
    phone VARCHAR(20),
    created_at DATETIME,
    status VARCHAR(20),
    _extraction_time DATETIME DEFAULT GETDATE()
);

-- dbt handles comprehensive validation
-- File: models/marts/dim_customers.sql
{{ config(
    materialized='table',
    post_hook="ALTER TABLE {{ this }} ADD CONSTRAINT pk_dim_customers PRIMARY KEY (customer_id)"
) }}

WITH validated_customers AS (
    SELECT *
    FROM {{ ref('stg_customers') }}
    WHERE customer_id IS NOT NULL
      AND email IS NOT NULL
      AND email REGEXP '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
)

SELECT 
    customer_id,
    email,
    first_name,
    last_name,
    phone,
    created_at,
    status
FROM validated_customers

-- =====================================
-- PHASE 7: MONITORING & ALERTING
-- =====================================

-- Step 7A: Set up data quality monitoring
-- File: models/monitoring/data_quality_summary.sql
{{ config(materialized='table') }}

WITH test_results AS (
    SELECT 
        'customers' as table_name,
        'unique_customer_id' as test_name,
        CASE WHEN COUNT(*) = COUNT(DISTINCT customer_id) 
             THEN 'PASS' ELSE 'FAIL' END as test_result,
        COUNT(*) as total_records,
        COUNT(DISTINCT customer_id) as unique_records
    FROM {{ ref('stg_customers') }}
)

SELECT 
    table_name,
    test_name,
    test_result,
    total_records,
    unique_records,
    CURRENT_TIMESTAMP as checked_at
FROM test_results

-- Step 7B: Automated data quality alerts
-- File: tests/monitoring/critical_data_quality.sql
{{ config(
    severity='error',
    store_failures=true
) }}

-- Alert on critical data quality issues
SELECT 
    'CRITICAL' as severity,
    'Duplicate customer IDs detected' as issue,
    COUNT(*) as failure_count
FROM (
    SELECT customer_id, COUNT(*) as cnt
    FROM {{ ref('stg_customers') }}
    GROUP BY customer_id
    HAVING COUNT(*) > 1
) duplicates

-- =====================================
-- PHASE 8: PERFORMANCE OPTIMIZATION
-- =====================================

-- Step 8A: Optimize test performance
-- Use dbt's incremental testing for large tables
-- File: dbt_project.yml
/*
data_tests:
  +store_failures: true
  +store_failures_as: "test_failures"
  
models:
  my_project:
    staging:
      +materialized: table
      +data_tests:
        +limit: 100  # Limit test sample size for performance
*/

-- Step 8B: Smart test scheduling
-- Critical tests run on every build
-- Comprehensive tests run nightly
/*
# profiles.yml - separate test environments
data_quality_full:
  outputs:
    prod:
      # Full test suite
    dev: 
      # Subset for development
*/