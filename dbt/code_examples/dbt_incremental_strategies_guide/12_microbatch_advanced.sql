-- dbt Microbatch Strategy - Advanced Configuration
-- Hourly batches with lookback and session calculations

{{
  config(
    materialized='incremental',
    incremental_strategy='microbatch',
    event_time='event_timestamp',
    batch_size='hour',
    lookback='3 hours'  -- Reprocess last 3 hours for late data
  )
}}

with event_metrics as (
  select
    event_id,
    user_id,
    event_type,
    event_timestamp,
    
    -- Calculate session boundaries (cross-batch calculations)
    lag(event_timestamp, 1, '1900-01-01'::timestamp) over (
      partition by user_id 
      order by event_timestamp
    ) as prev_event_timestamp,
    
    case when 
      event_timestamp - lag(event_timestamp) over (
        partition by user_id 
        order by event_timestamp
      ) > interval '30 minutes'
    then 1 else 0 end as new_session_flag
    
  from {{ ref('stg_events') }}
)

select
  *,
  sum(new_session_flag) over (
    partition by user_id 
    order by event_timestamp 
    rows unbounded preceding
  ) as session_id
from event_metrics