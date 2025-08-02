-- dbt Append Strategy - Safe Patterns to Prevent Duplicates
-- Examples of how to safely use append strategy

-- Pattern 1: Natural Uniqueness with Immutable Events
{{
  config(
    materialized='incremental',
    incremental_strategy='append'
  )
}}

select
  {{ dbt_utils.generate_surrogate_key([
    'user_id', 'event_timestamp', 'event_type'
  ]) }} as event_id,           -- Generate unique ID
  user_id,
  event_type,
  event_timestamp,
  properties
from {{ ref('stg_events') }}

{% if is_incremental() %}
  where event_timestamp > (select max(event_timestamp) from {{ this }})
  -- Add safety check to prevent duplicates
  and {{ dbt_utils.generate_surrogate_key([
    'user_id', 'event_timestamp', 'event_type'
  ]) }} not in (select event_id from {{ this }})
{% endif %}

-- Pattern 2: Deduplication Before Append
{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    pre_hook="delete from {{ this }} where created_date = current_date"  -- Clean today's data first
  )
}}

with deduplicated_data as (
  select
    transaction_id,
    customer_id,
    amount,
    transaction_timestamp,
    date(transaction_timestamp) as created_date,
    
    -- Remove duplicates within the batch
    row_number() over (
      partition by transaction_id 
      order by transaction_timestamp desc
    ) as rn
    
  from {{ ref('stg_transactions') }}
  
  {% if is_incremental() %}
    where date(transaction_timestamp) >= current_date - interval '1' day
  {% endif %}
)

select * from deduplicated_data where rn = 1