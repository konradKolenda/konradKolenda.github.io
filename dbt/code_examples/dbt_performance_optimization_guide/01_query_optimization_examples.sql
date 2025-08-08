-- Query Optimization Examples for dbt Performance Guide
-- These examples demonstrate key SQL optimization techniques for dbt models

-- ============================================================================
-- EXAMPLE 1: Predicate Pushdown Optimization
-- ============================================================================

-- ❌ BEFORE: Inefficient - processes all data first
{{ config(
    materialized='table',
    alias='customer_metrics_slow'
) }}

select 
    customer_id,
    order_date,
    revenue,
    -- Complex calculations on full dataset
    avg(revenue) over (
        partition by customer_id 
        order by order_date 
        rows between 30 preceding and current row
    ) as rolling_30_day_avg,
    
    sum(revenue) over (
        partition by customer_id 
        order by order_date
    ) as cumulative_revenue
    
from {{ ref('fact_orders') }}
where order_date >= '2023-01-01'  -- Late filtering after expensive calculations
  and status = 'completed'


-- ✅ AFTER: Optimized - filters early, reduces compute
{{ config(
    materialized='incremental',
    unique_key='order_id',
    alias='customer_metrics_optimized'
) }}

with filtered_orders as (
    -- Apply filters FIRST to reduce data volume
    select 
        customer_id,
        order_date,
        revenue,
        order_id
    from {{ ref('fact_orders') }}
    where order_date >= '2023-01-01'  -- Early filtering
      and status = 'completed'
      {% if is_incremental() %}
        and order_date > (select max(order_date) from {{ this }})
      {% endif %}
),

-- Now perform expensive calculations on reduced dataset
enriched_orders as (
    select 
        *,
        avg(revenue) over (
            partition by customer_id 
            order by order_date 
            rows between 30 preceding and current row
        ) as rolling_30_day_avg,
        
        sum(revenue) over (
            partition by customer_id 
            order by order_date
        ) as cumulative_revenue
    from filtered_orders
)

select * from enriched_orders


-- ============================================================================
-- EXAMPLE 2: Join Optimization Strategy
-- ============================================================================

-- ❌ BEFORE: Inefficient join order
select 
    o.order_id,
    c.customer_name,
    p.product_name,
    ol.quantity,
    ol.price
from {{ ref('fact_orders') }} o
left join {{ ref('dim_customers') }} c on o.customer_id = c.customer_id  -- Large table joined early
left join {{ ref('fact_order_lines') }} ol on o.order_id = ol.order_id   -- Explodes row count
left join {{ ref('dim_products') }} p on ol.product_id = p.product_id    -- Joined after explosion
where o.order_date >= current_date - 30
  and c.customer_segment = 'premium'


-- ✅ AFTER: Optimized join order and strategy
with recent_orders as (
    -- Filter orders first
    select order_id, customer_id, order_date
    from {{ ref('fact_orders') }}
    where order_date >= current_date - 30
),

premium_customers as (
    -- Filter customers early
    select customer_id, customer_name
    from {{ ref('dim_customers') }}
    where customer_segment = 'premium'
),

-- Pre-aggregate order lines to reduce join explosion
order_lines_summary as (
    select 
        ol.order_id,
        count(*) as line_count,
        sum(ol.quantity * ol.price) as total_amount,
        listagg(p.product_name, ', ') as product_names
    from {{ ref('fact_order_lines') }} ol
    inner join {{ ref('dim_products') }} p on ol.product_id = p.product_id
    where ol.order_id in (select order_id from recent_orders)
    group by ol.order_id
),

-- Final join with smaller, pre-filtered datasets
final as (
    select 
        ro.order_id,
        pc.customer_name,
        ro.order_date,
        ols.line_count,
        ols.total_amount,
        ols.product_names
    from recent_orders ro
    inner join premium_customers pc on ro.customer_id = pc.customer_id
    left join order_lines_summary ols on ro.order_id = ols.order_id
)

select * from final


-- ============================================================================
-- EXAMPLE 3: Aggregation Optimization
-- ============================================================================

-- ❌ BEFORE: Memory-intensive multiple window functions
select 
    customer_id,
    order_date,
    sum(revenue) as daily_revenue,
    count(distinct order_id) as order_count,
    -- Multiple expensive window functions
    rank() over (partition by customer_id order by sum(revenue) desc) as revenue_rank,
    row_number() over (partition by customer_id order by order_date desc) as recency_rank,
    avg(revenue) over (partition by customer_id order by order_date rows between 6 preceding and current row) as rolling_7d_avg,
    sum(revenue) over (partition by customer_id order by order_date rows unbounded preceding) as cumulative_revenue
from {{ ref('fact_orders') }}
group by customer_id, order_date
having sum(revenue) > 1000


-- ✅ AFTER: Optimized with staged calculations
with daily_aggregates as (
    -- First stage: basic aggregation with early filtering
    select 
        customer_id,
        order_date::date as order_date,
        sum(revenue) as daily_revenue,
        count(distinct order_id) as order_count,
        avg(revenue) as avg_order_value
    from {{ ref('fact_orders') }}
    group by customer_id, order_date::date
    having sum(revenue) > 1000  -- Filter early to reduce window function scope
),

-- Second stage: customer-level rankings (smaller dataset)
customer_rankings as (
    select 
        *,
        rank() over (partition by customer_id order by daily_revenue desc) as revenue_rank
    from daily_aggregates
),

-- Third stage: temporal calculations on ranked subset
final_metrics as (
    select 
        *,
        row_number() over (partition by customer_id order by order_date desc) as recency_rank,
        
        -- Window functions on pre-filtered, smaller dataset
        avg(daily_revenue) over (
            partition by customer_id 
            order by order_date 
            rows between 6 preceding and current row
        ) as rolling_7d_avg,
        
        sum(daily_revenue) over (
            partition by customer_id 
            order by order_date 
            rows unbounded preceding
        ) as cumulative_revenue
        
    from customer_rankings
    where revenue_rank <= 10  -- Only calculate for top performers
)

select * from final_metrics


-- ============================================================================
-- EXAMPLE 4: Set Operations Optimization
-- ============================================================================

-- ❌ BEFORE: Inefficient UNION with duplicates processing
select customer_id, order_date, 'online' as channel, revenue from online_orders
union  -- Forces duplicate removal - expensive
select customer_id, order_date, 'retail' as channel, revenue from retail_orders
union
select customer_id, order_date, 'mobile' as channel, revenue from mobile_orders


-- ✅ AFTER: Optimized UNION ALL with controlled deduplication
with all_orders as (
    -- Use UNION ALL (faster than UNION)
    select customer_id, order_date, 'online' as channel, revenue, 1 as priority from online_orders
    union all
    select customer_id, order_date, 'retail' as channel, revenue, 2 as priority from retail_orders
    union all
    select customer_id, order_date, 'mobile' as channel, revenue, 3 as priority from mobile_orders
),

-- Strategic deduplication with business logic
deduplicated as (
    select 
        customer_id,
        order_date,
        channel,
        revenue,
        row_number() over (
            partition by customer_id, order_date
            order by priority  -- Prefer online > mobile > retail
        ) as row_num
    from all_orders
)

select customer_id, order_date, channel, revenue
from deduplicated
where row_num = 1