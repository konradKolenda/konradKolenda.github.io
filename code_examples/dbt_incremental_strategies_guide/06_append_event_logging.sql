-- dbt Append Strategy - Event Logging Example
-- Perfect use case with partitioning and proper event structure

{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    partition_by={'field': 'event_date', 'data_type': 'date'}
  )
}}

select
  event_id,                    -- Naturally unique (UUID, timestamp-based)
  user_id,
  event_type,
  event_timestamp,
  date(event_timestamp) as event_date,
  session_id,
  page_url,
  referrer,
  user_agent,
  properties                   -- JSON/variant column
from {{ ref('stg_web_events') }}

{% if is_incremental() %}
  where event_timestamp > (select max(event_timestamp) from {{ this }})
{% endif %}