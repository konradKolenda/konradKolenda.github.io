-- dbt Merge Strategy - Advanced with Deletion Support
-- Includes custom merge logic and deletion handling

{{
  config(
    materialized='incremental',
    unique_key='customer_id',
    incremental_strategy='merge',
    
    -- Exclude certain columns from being updated
    merge_exclude_columns=['created_at', 'first_seen'],
    
    -- Add deletion logic
    merge_update_columns={
      'customer_name': 'source.customer_name',
      'email': 'source.email',
      'is_active': 'source.is_active'
    }
  )
}}

with source_with_deletes as (
  select
    customer_id,
    customer_name,
    email,
    phone,
    is_active,
    last_updated,
    
    -- Mark records for deletion
    case 
      when is_active = false then true 
      else false 
    end as _should_delete
    
  from {{ ref('stg_customers') }}
  
  {% if is_incremental() %}
    where last_updated > (select max(last_updated) from {{ this }})
       or is_active = false  -- Always include records marked for deletion
  {% endif %}
)

select * from source_with_deletes

-- ADVANCED: Custom merge with deletion (warehouse-specific)
-- BigQuery example:
-- MERGE target T USING source S ON T.customer_id = S.customer_id
-- WHEN MATCHED AND S._should_delete = true THEN DELETE
-- WHEN MATCHED AND S._should_delete = false THEN UPDATE SET ...
-- WHEN NOT MATCHED AND S._should_delete = false THEN INSERT ...