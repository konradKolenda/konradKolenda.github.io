-- Materialization Optimization Examples for dbt Performance Guide
-- These examples show how to choose and optimize different materialization strategies

-- ============================================================================
-- EXAMPLE 1: Microbatch Processing for Time-Series Data
-- ============================================================================

{{ config(
    materialized='incremental',
    incremental_strategy='microbatch',
    event_time='order_timestamp',
    batch_size='day',
    lookback=2,
    alias='customer_daily_metrics_microbatch'
) }}

select 
    customer_id,
    order_timestamp::date as order_date,
    order_timestamp,
    revenue,
    
    -- Window functions work efficiently within microbatches
    sum(revenue) over (
        partition by customer_id 
        order by order_timestamp 
        range between interval '7 days' preceding and current row
    ) as revenue_7d_rolling,
    
    -- Count orders in current day (microbatch scope)
    count(*) over (
        partition by customer_id, order_timestamp::date
    ) as daily_order_count,
    
    -- Running total within batch window
    sum(revenue) over (
        partition by customer_id 
        order by order_timestamp 
        rows unbounded preceding
    ) as running_total

from {{ ref('raw_orders') }}
where order_timestamp >= date('2023-01-01')


-- ============================================================================
-- EXAMPLE 2: Smart Incremental Merge with Change Detection
-- ============================================================================

{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='customer_id',
    merge_exclude_columns=['last_updated', 'data_hash'],
    alias='customer_profile_smart_merge'
) }}

with source_data as (
    select 
        customer_id,
        customer_name,
        email,
        phone,
        registration_date,
        last_purchase_date,
        total_lifetime_value,
        customer_segment,
        current_timestamp() as last_updated,
        
        -- Create hash for efficient change detection
        md5(concat_ws('||',
            coalesce(customer_name, ''),
            coalesce(email, ''),
            coalesce(phone, ''),
            coalesce(last_purchase_date::string, ''),
            coalesce(total_lifetime_value::string, ''),
            coalesce(customer_segment, '')
        )) as data_hash
        
    from {{ ref('raw_customers') }}
    
    {% if is_incremental() %}
        -- Only process potentially changed records
        where last_purchase_date >= (
            select max(last_purchase_date) - interval '7 days' 
            from {{ this }}
        )
        or customer_id not in (select customer_id from {{ this }})
    {% endif %}
),

-- Identify only truly changed records for merge efficiency
{% if is_incremental() %}
changed_records as (
    select s.*
    from source_data s
    left join {{ this }} t using (customer_id)
    where t.customer_id is null  -- New records
       or s.data_hash != t.data_hash  -- Changed records only
)

select 
    customer_id,
    customer_name,
    email,
    phone,
    registration_date,
    last_purchase_date,
    total_lifetime_value,
    customer_segment,
    last_updated
from changed_records

{% else %}
-- Initial load: all records
select 
    customer_id,
    customer_name,
    email,
    phone,
    registration_date,
    last_purchase_date,
    total_lifetime_value,
    customer_segment,
    last_updated
from source_data
{% endif %}


-- ============================================================================
-- EXAMPLE 3: Hybrid Materialization Strategy
-- ============================================================================

-- Strategy: Use different materializations for different access patterns

-- Hot data: Table materialization for frequently accessed recent data
{{ config(
    materialized='table',
    alias='orders_hot_data'
) }}

select 
    order_id,
    customer_id,
    order_date,
    revenue,
    product_category,
    order_status
from {{ ref('raw_orders') }}
where order_date >= current_date - 90  -- Last 90 days only
  and order_status in ('completed', 'shipped')


-- Warm data: Incremental for historical analysis
{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='append',
    alias='orders_warm_data'
) }}

select 
    order_id,
    customer_id,
    order_date,
    revenue,
    product_category
from {{ ref('raw_orders') }}
where order_date < current_date - 90
  and order_date >= '2020-01-01'

{% if is_incremental() %}
  -- Only append newly aged-out data from hot storage
  and order_date >= current_date - 93
  and order_date < current_date - 90
{% endif %}


-- Cold data: View for rarely accessed historical data
{{ config(
    materialized='view',
    alias='orders_cold_data'
) }}

select 
    order_id,
    customer_id,
    order_date,
    revenue,
    'archived' as data_tier
from {{ ref('raw_orders') }}
where order_date < '2020-01-01'


-- ============================================================================
-- EXAMPLE 4: Performance-Optimized Snapshot Strategy
-- ============================================================================

{% snapshot customer_scd_optimized %}
    {{
        config(
            target_database='analytics',
            target_schema='snapshots',
            unique_key='customer_id',
            strategy='timestamp',
            updated_at='last_modified_at',
            
            -- Performance optimizations
            pre_hook=[
                -- Analyze source table before snapshot
                "analyze table {{ source('crm', 'customers') }}",
                
                -- Create temporary indexes for performance
                "create index if not exists tmp_customer_modified_idx on {{ source('crm', 'customers') }} (last_modified_at)",
                "create index if not exists tmp_customer_id_idx on {{ source('crm', 'customers') }} (customer_id)"
            ],
            
            post_hook=[
                -- Clean up temporary indexes
                "drop index if exists tmp_customer_modified_idx",
                "drop index if exists tmp_customer_id_idx",
                
                -- Update statistics on snapshot table
                "analyze table {{ this }}"
            ]
        )
    }}

    select 
        customer_id,
        customer_name,
        email,
        customer_segment,
        total_lifetime_value,
        last_modified_at,
        
        -- Add hash for faster comparison
        md5(concat_ws('||',
            customer_name,
            email, 
            customer_segment,
            total_lifetime_value::string
        )) as record_hash
        
    from {{ source('crm', 'customers') }}
    
    -- Only process records modified since last snapshot
    {% if is_incremental() %}
        where last_modified_at > (
            select max(last_modified_at) 
            from {{ this }} 
            where dbt_valid_to is null
        )
    {% endif %}

{% endsnapshot %}


-- ============================================================================
-- EXAMPLE 5: Conditional Materialization Based on Data Volume
-- ============================================================================

{% macro get_table_size(relation) %}
    {% set size_query %}
        select count(*) as row_count
        from {{ relation }}
    {% endset %}
    
    {% if execute %}
        {% set results = run_query(size_query) %}
        {% if results %}
            {{ return(results.columns[0].values()[0]) }}
        {% endif %}
    {% endif %}
    {{ return(0) }}
{% endmacro %}

-- Dynamic materialization based on data size
{% set source_size = get_table_size(ref('staging_orders')) %}

{{ config(
    materialized={% if source_size > 10000000 %}'incremental'{% elif source_size > 1000000 %}'table'{% else %}'view'{% endif %},
    
    {% if source_size > 10000000 %}
        unique_key='order_id',
        incremental_strategy='merge'
    {% endif %}
) }}

select 
    order_id,
    customer_id,
    order_date,
    revenue,
    
    -- Add metadata about materialization decision
    '{{ source_size }}' as source_row_count,
    '{{ config.get('materialized') }}' as chosen_materialization
    
from {{ ref('staging_orders') }}

{% if config.get('materialized') == 'incremental' and is_incremental() %}
    where order_date > (select max(order_date) from {{ this }})
{% endif %}


-- ============================================================================
-- EXAMPLE 6: Materialization Performance Monitoring
-- ============================================================================

{% macro log_materialization_performance() %}
    {% if execute %}
        {% set performance_log %}
            create table if not exists {{ target.database }}.analytics.materialization_performance (
                model_name varchar,
                materialization varchar,
                execution_time_seconds float,
                rows_processed bigint,
                data_freshness_hours float,
                cost_estimate_usd float,
                run_timestamp timestamp
            );
            
            insert into {{ target.database }}.analytics.materialization_performance
            values (
                '{{ this.identifier }}',
                '{{ config.get('materialized') }}',
                {{ execution_time | default(0) }},
                (select count(*) from {{ this }}),
                extract(epoch from (current_timestamp - (select max(updated_at) from {{ this }}))) / 3600,
                {{ (execution_time | default(0) / 3600) * 2.50 }},  -- Estimate at $2.50/hour
                current_timestamp
            );
        {% endset %}
        
        {{ run_query(performance_log) }}
    {% endif %}
{% endmacro %}

-- Usage in model
{{ config(
    materialized='incremental',
    post_hook="{{ log_materialization_performance() }}"
) }}

select * from {{ ref('source_table') }}