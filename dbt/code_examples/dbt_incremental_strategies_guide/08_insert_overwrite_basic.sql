-- dbt Insert Overwrite Strategy - Basic Implementation
-- Completely replaces entire partitions with new data

{{
  config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={'field': 'order_date', 'data_type': 'date'}
  )
}}

with daily_orders as (
  select
    order_id,
    customer_id,
    order_amount,
    order_status,
    order_timestamp,
    date(order_timestamp) as order_date
  from {{ ref('stg_orders') }}
  
  {% if is_incremental() %}
    -- Only process recent dates (will overwrite entire partitions)
    where date(order_timestamp) >= current_date - interval '7' day
  {% endif %}
)

select * from daily_orders

-- BEHIND THE SCENES:
-- 1. Identifies which partitions contain new data
-- 2. Drops those entire partitions
-- 3. Inserts all new data for those partitions