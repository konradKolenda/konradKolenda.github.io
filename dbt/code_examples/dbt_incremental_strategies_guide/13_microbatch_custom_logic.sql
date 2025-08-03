-- dbt Microbatch Strategy - Custom Batch Processing Logic
-- Advanced example with custom macros for batch boundaries

{{
  config(
    materialized='incremental',
    incremental_strategy='microbatch',
    event_time='created_at',
    batch_size='day',
    
    -- Custom batch logic
    begin_batch_macro='get_batch_start',
    end_batch_macro='get_batch_end'
  )
}}

select
  transaction_id,
  customer_id,
  amount,
  transaction_type,
  created_at,
  
  -- Calculate running totals within batch
  sum(amount) over (
    partition by customer_id, date(created_at)
    order by created_at
    rows unbounded preceding
  ) as running_daily_total
  
from {{ ref('stg_transactions') }}

-- Custom macros would be defined in macros/batch_logic.sql:
--
-- {% macro get_batch_start() %}
--   select min(created_at) from {{ ref('stg_source') }}
--   where date(created_at) = '{{ var("batch_date") }}'
-- {% endmacro %}
--
-- {% macro get_batch_end() %}
--   select max(created_at) from {{ ref('stg_source') }}
--   where date(created_at) = '{{ var("batch_date") }}'
-- {% endmacro %}