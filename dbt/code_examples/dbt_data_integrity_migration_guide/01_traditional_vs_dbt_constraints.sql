-- ================================================================
-- Traditional SQL Server vs dbt Data Integrity Approaches
-- ================================================================

-- =====================================
-- 1. TRADITIONAL SQL SERVER APPROACH
-- =====================================

/*
-- Primary Key Constraint
CREATE TABLE customers (
    customer_id INT IDENTITY(1,1) PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    phone VARCHAR(20),
    created_at DATETIME DEFAULT GETDATE(),
    status VARCHAR(20) DEFAULT 'active' 
        CHECK (status IN ('active', 'inactive', 'suspended'))
);

-- Foreign Key Constraints
CREATE TABLE orders (
    order_id INT IDENTITY(1,1) PRIMARY KEY,
    customer_id INT NOT NULL,
    order_date DATETIME DEFAULT GETDATE(),
    total_amount DECIMAL(10,2) CHECK (total_amount > 0),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

-- Composite Primary Key
CREATE TABLE order_items (
    order_id INT,
    product_id INT,
    quantity INT CHECK (quantity > 0),
    unit_price DECIMAL(10,2) CHECK (unit_price >= 0),
    PRIMARY KEY (order_id, product_id),
    FOREIGN KEY (order_id) REFERENCES orders(order_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);
*/

-- =====================================
-- 2. DBT EQUIVALENT APPROACH
-- =====================================

-- File: models/staging/stg_customers.sql
-- =====================================
{{ config(materialized='table') }}

SELECT 
    customer_id,
    LOWER(TRIM(email)) as email,
    TRIM(first_name) as first_name,
    TRIM(last_name) as last_name,
    phone,
    created_at,
    COALESCE(status, 'active') as status
FROM {{ source('raw', 'raw_customers') }}

-- File: models/staging/stg_orders.sql
-- =====================================
{{ config(materialized='table') }}

SELECT 
    order_id,
    customer_id,
    order_date,
    total_amount
FROM {{ source('raw', 'raw_orders') }}
WHERE total_amount > 0  -- Business rule validation

-- File: models/staging/stg_order_items.sql
-- =====================================
{{ config(materialized='table') }}

SELECT 
    order_id,
    product_id,
    quantity,
    unit_price
FROM {{ source('raw', 'raw_order_items') }}
WHERE quantity > 0 
  AND unit_price >= 0  -- Business rule validation