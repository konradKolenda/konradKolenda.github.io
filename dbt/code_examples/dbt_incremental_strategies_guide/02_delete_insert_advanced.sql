-- dbt Delete+Insert Strategy - Advanced Configuration
-- Includes merge exclusions, incremental predicates, and change detection

{{
  config(
    materialized='incremental',
    unique_key='customer_id',
    incremental_strategy='delete+insert',
    
    -- Control which columns to exclude from updates
    merge_exclude_columns=['created_at', 'inserted_by'],
    
    -- Add conditions to the DELETE statement
    incremental_predicates=[
      "DBT_INTERNAL_DEST.last_updated < current_timestamp - interval '30 days'"
    ]
  )
}}

-- Example: Only update records that have actually changed
select
  customer_id,
  customer_name,
  email,
  phone,
  last_updated,
  
  -- Create a hash to detect actual changes
  {{ dbt_utils.generate_surrogate_key([
    'customer_name', 'email', 'phone'
  ]) }} as row_hash
  
from {{ ref('stg_customers') }}

{% if is_incremental() %}
  where last_updated > (select max(last_updated) from {{ this }})
  -- Only include records where content actually changed
  and {{ dbt_utils.generate_surrogate_key([
    'customer_name', 'email', 'phone'
  ]) }} not in (
    select row_hash from {{ this }}
    where customer_id in (
      select customer_id from {{ ref('stg_customers') }}
      where last_updated > (select max(last_updated) from {{ this }})
    )
  )
{% endif %}