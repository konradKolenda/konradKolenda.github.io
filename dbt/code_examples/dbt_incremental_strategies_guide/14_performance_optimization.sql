-- Performance Optimization Tips for All Incremental Strategies
-- Universal best practices for better performance

{{
  config(
    materialized='incremental',
    unique_key='customer_id',
    incremental_strategy='merge',  -- Choose appropriate strategy
    
    -- Performance optimizations
    partition_by={'field': 'created_date', 'data_type': 'date'},
    cluster_by=['status', 'customer_id'],  -- Frequently filtered columns
    
    -- Monitor performance
    post_hook=[
      "insert into metadata.model_performance 
       select '{{ this }}', current_timestamp, 
       '{{ invocation_id }}', {{ query_tag() }}"
    ]
  )
}}

with source_data as (
  select
    customer_id,
    customer_name,
    email,
    status,
    created_date,
    updated_at,
    
    -- Add data quality validations
    case when customer_id is null then 'INVALID' else 'VALID' end as data_quality_flag
    
  from {{ ref('stg_customers') }}
  
  -- Data quality filters
  where customer_id is not null  -- Prevent NULL unique_key issues
    and created_date is not null  -- Prevent partition issues
    and email is not null         -- Business rule validation
)

select * from source_data

{% if is_incremental() %}
  where updated_at > (select max(updated_at) from {{ this }})
  -- Add buffer for late-arriving data
  and updated_at > current_timestamp - interval '1 hour'
  -- Only process valid records
  and data_quality_flag = 'VALID'
{% else %}
  -- Full refresh logic with appropriate batch sizing
  {% if var('full_refresh', false) %}
    -- Process all data
  {% else %}
    -- Initial load: process last N days to handle late data
    where created_date >= current_date - {{ var('lookback_days', 7) }}
  {% endif %}
{% endif %}