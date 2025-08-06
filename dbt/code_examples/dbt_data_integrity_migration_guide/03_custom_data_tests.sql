-- ================================================================
-- Custom Data Tests - Complex Business Rules
-- Equivalent to complex CHECK constraints and business logic
-- ================================================================

-- =====================================
-- 1. PRICE CONSISTENCY TEST
-- Traditional: CHECK (discount_price <= price AND discount_price >= 0)
-- =====================================

-- File: tests/assert_price_consistency.sql
{{ config(severity='error') }}

SELECT 
    product_id,
    sku,
    price,
    discount_price,
    'Discount price must be less than or equal to regular price and non-negative' as error_message
FROM {{ ref('products') }}
WHERE discount_price > price 
   OR discount_price < 0

-- =====================================
-- 2. ORDER DATE VALIDATION
-- Traditional: CHECK (order_date <= GETDATE())
-- =====================================

-- File: tests/assert_order_date_not_future.sql
{{ config(severity='error') }}

SELECT 
    order_id,
    customer_id,
    order_date,
    'Order date cannot be in the future' as error_message
FROM {{ ref('stg_orders') }}
WHERE order_date > {{ dbt.current_timestamp() }}

-- =====================================
-- 3. EMAIL DOMAIN VALIDATION
-- Traditional: Complex CHECK constraint with LIKE patterns
-- =====================================

-- File: tests/assert_valid_email_domains.sql
{{ config(severity='warn') }}

WITH blocked_domains AS (
    SELECT domain FROM (
        VALUES 
            ('tempmail.com'),
            ('10minutemail.com'),
            ('guerrillamail.com')
    ) AS t(domain)
),

customer_domains AS (
    SELECT 
        customer_id,
        email,
        LOWER(SUBSTRING(email, CHARINDEX('@', email) + 1, LEN(email))) as email_domain
    FROM {{ ref('stg_customers') }}
    WHERE email IS NOT NULL 
      AND CHARINDEX('@', email) > 0
)

SELECT 
    cd.customer_id,
    cd.email,
    cd.email_domain,
    'Customer using blocked email domain' as error_message
FROM customer_domains cd
INNER JOIN blocked_domains bd ON cd.email_domain = bd.domain

-- =====================================
-- 4. REFERENTIAL INTEGRITY WITH CONDITIONS
-- Traditional: Foreign key with additional conditions
-- =====================================

-- File: tests/assert_active_customer_orders.sql
{{ config(severity='error') }}

SELECT 
    o.order_id,
    o.customer_id,
    c.status as customer_status,
    'Orders cannot be placed by inactive customers' as error_message
FROM {{ ref('stg_orders') }} o
LEFT JOIN {{ ref('stg_customers') }} c ON o.customer_id = c.customer_id
WHERE c.status != 'active' 
   OR c.customer_id IS NULL

-- =====================================
-- 5. INVENTORY VALIDATION
-- Traditional: Complex trigger or stored procedure logic
-- =====================================

-- File: tests/assert_sufficient_inventory.sql
{{ config(severity='warn') }}

WITH order_quantities AS (
    SELECT 
        product_id,
        SUM(quantity) as total_ordered
    FROM {{ ref('stg_order_items') }} oi
    INNER JOIN {{ ref('stg_orders') }} o ON oi.order_id = o.order_id
    WHERE o.order_date >= DATEADD(day, -30, GETDATE())  -- Last 30 days
    GROUP BY product_id
)

SELECT 
    oq.product_id,
    p.sku,
    oq.total_ordered,
    p.inventory_quantity,
    'Product may have insufficient inventory for recent orders' as warning_message
FROM order_quantities oq
INNER JOIN {{ ref('products') }} p ON oq.product_id = p.product_id
WHERE oq.total_ordered > p.inventory_quantity * 0.8  -- 80% threshold

-- =====================================
-- 6. DATA COMPLETENESS TEST
-- Traditional: Multiple NOT NULL constraints
-- =====================================

-- File: tests/assert_customer_profile_completeness.sql
{{ config(severity='warn') }}

SELECT 
    customer_id,
    email,
    first_name,
    last_name,
    phone,
    'Customer profile is incomplete' as warning_message
FROM {{ ref('stg_customers') }}
WHERE first_name IS NULL 
   OR last_name IS NULL 
   OR phone IS NULL
   OR LEN(TRIM(first_name)) = 0
   OR LEN(TRIM(last_name)) = 0

-- =====================================
-- 7. CROSS-TABLE BUSINESS RULE
-- Traditional: Multi-table CHECK constraint or trigger
-- =====================================

-- File: tests/assert_order_total_matches_items.sql
{{ config(severity='error') }}

WITH order_totals AS (
    SELECT 
        order_id,
        SUM(quantity * unit_price) as calculated_total
    FROM {{ ref('stg_order_items') }}
    GROUP BY order_id
)

SELECT 
    o.order_id,
    o.total_amount as recorded_total,
    ot.calculated_total,
    ABS(o.total_amount - ot.calculated_total) as difference,
    'Order total does not match sum of line items' as error_message
FROM {{ ref('stg_orders') }} o
INNER JOIN order_totals ot ON o.order_id = ot.order_id
WHERE ABS(o.total_amount - ot.calculated_total) > 0.01  -- Allow for rounding

-- =====================================
-- 8. TIME-BASED VALIDATION
-- Traditional: CHECK constraint with date functions
-- =====================================

-- File: tests/assert_reasonable_order_timing.sql
{{ config(severity='warn') }}

SELECT 
    customer_id,
    COUNT(*) as order_count,
    MIN(order_date) as first_order,
    MAX(order_date) as last_order,
    'Customer placed unusual number of orders in short time' as warning_message
FROM {{ ref('stg_orders') }}
WHERE order_date >= DATEADD(hour, -1, GETDATE())  -- Last hour
GROUP BY customer_id
HAVING COUNT(*) > 5  -- More than 5 orders in an hour