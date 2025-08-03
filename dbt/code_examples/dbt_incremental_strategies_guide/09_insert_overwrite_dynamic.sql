-- dbt Insert Overwrite Strategy - Dynamic Partition Management
-- Advanced partition overwrite with dynamic date detection

{{
  config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={
      'field': 'event_date',
      'data_type': 'date',
      'granularity': 'day'
    }
  )
}}

with processed_events as (
  select
    event_id,
    user_id,
    event_type,
    event_timestamp,
    date(event_timestamp) as event_date,
    
    -- Calculate derived metrics that might change with reprocessing
    count(*) over (
      partition by user_id, date(event_timestamp)
    ) as daily_event_count,
    
    lag(event_timestamp) over (
      partition by user_id 
      order by event_timestamp
    ) as previous_event_timestamp
    
  from {{ ref('stg_events') }}
  
  {% if is_incremental() %}
    where date(event_timestamp) in (
      -- Dynamically determine which partitions to overwrite
      {% set overwrite_dates %}
        select distinct date(event_timestamp)
        from {{ ref('stg_events') }}
        where _loaded_at > (
          select max(_loaded_at) 
          from {{ this }}
        )
      {% endset %}
      
      {{ dbt_utils.get_query_results_as_dict(overwrite_dates)['date'] | join(', ') }}
    )
  {% endif %}
)

select * from processed_events