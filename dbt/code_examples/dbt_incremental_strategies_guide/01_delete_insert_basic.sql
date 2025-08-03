-- dbt Delete+Insert Strategy - Basic Implementation
-- This is the default incremental strategy in dbt

{{
  config(
    materialized='incremental',
    unique_key='customer_id'  -- Required for updates
    -- incremental_strategy='delete+insert' -- This is the default
  )
}}

with incremental_data as (
  select
    customer_id,
    customer_name,
    email,
    phone,
    last_updated
  from {{ ref('stg_customers') }}
  
  {% if is_incremental() %}
    -- Only process changed records
    where last_updated > (select max(last_updated) from {{ this }})
  {% endif %}
)

select * from incremental_data

-- BEHIND THE SCENES:
-- Step 1: DELETE FROM target WHERE customer_id IN (1, 2, 3, ...)
-- Step 2: INSERT INTO target SELECT * FROM incremental_data