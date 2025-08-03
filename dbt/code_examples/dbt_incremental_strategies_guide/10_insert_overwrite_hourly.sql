-- dbt Insert Overwrite Strategy - Hourly Partitioning
-- Example with hourly granularity for real-time data processing

{{
  config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={
      'field': 'event_hour',
      'data_type': 'timestamp',
      'granularity': 'hour'
    }
  )
}}

select
  event_id,
  user_id,
  event_timestamp,
  timestamp_trunc(event_timestamp, HOUR) as event_hour,
  properties
from {{ ref('stg_realtime_events') }}

{% if is_incremental() %}
  where timestamp_trunc(event_timestamp, HOUR) >= 
    timestamp_sub(current_timestamp(), interval 6 hour)
{% endif %}