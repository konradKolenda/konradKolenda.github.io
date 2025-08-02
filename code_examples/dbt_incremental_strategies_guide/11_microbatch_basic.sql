-- dbt Microbatch Strategy - Basic Implementation (dbt 1.9+)
-- Processes data in time-based batches automatically

{{
  config(
    materialized='incremental',
    incremental_strategy='microbatch',
    event_time='order_timestamp',
    batch_size='day'
  )
}}

select
  order_id,
  customer_id,
  order_amount,
  order_status,
  order_timestamp,
  
  -- Calculations are done per batch
  sum(order_amount) over (
    partition by customer_id, date(order_timestamp)
  ) as daily_customer_total
  
from {{ ref('stg_orders') }}

-- dbt automatically handles:
-- 1. Splitting data into daily batches
-- 2. Processing each batch incrementally
-- 3. Handling overlapping data between batches