-- dbt Append Strategy - Basic Implementation
-- Simply adds new rows without deduplication - perfect for event logs

{{
  config(
    materialized='incremental',
    incremental_strategy='append'
    -- unique_key is IGNORED with append strategy
  )
}}

with new_events as (
  select
    event_id,
    user_id,
    event_type,
    event_timestamp,
    properties
  from {{ ref('stg_events') }}
  
  {% if is_incremental() %}
    -- Only get events since last run
    where event_timestamp > (select max(event_timestamp) from {{ this }})
  {% endif %}
)

select * from new_events

-- BEHIND THE SCENES:
-- Simply: INSERT INTO target SELECT * FROM new_events
-- No DELETE operations, no deduplication