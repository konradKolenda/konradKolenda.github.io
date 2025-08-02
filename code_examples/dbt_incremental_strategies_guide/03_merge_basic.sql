-- dbt Merge Strategy - Basic Implementation
-- Uses database-native MERGE statements for optimal performance

{{
  config(
    materialized='incremental',
    unique_key='customer_id',
    incremental_strategy='merge'
  )
}}

select
  customer_id,
  customer_name,
  email,
  phone,
  subscription_status,
  last_updated
from {{ ref('stg_customers') }}

{% if is_incremental() %}
  where last_updated > (select max(last_updated) from {{ this }})
{% endif %}

-- GENERATES NATIVE MERGE STATEMENT:
-- MERGE target USING source ON target.customer_id = source.customer_id
-- WHEN MATCHED THEN UPDATE SET ...
-- WHEN NOT MATCHED THEN INSERT ...