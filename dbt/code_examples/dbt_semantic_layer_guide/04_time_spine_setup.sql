-- Time Spine Setup for dbt Semantic Layer
-- This model creates an optimized time spine for MetricFlow temporal calculations
-- Essential for cumulative metrics, time-based aggregations, and gap filling

{{ config(
    materialized='table',
    indexes=[
        {'columns': ['date_day'], 'type': 'btree'},
        {'columns': ['date_year', 'date_month'], 'type': 'btree'},
        {'columns': ['date_quarter'], 'type': 'btree'}
    ]
) }}

with date_spine as (
  {{ dbt_utils.date_spine(
      datepart="day",
      start_date="cast('2020-01-01' as date)",
      end_date="cast('2030-12-31' as date)"
     )
  }}
),

enhanced_time_spine as (
  select 
    date_day,
    
    -- Date hierarchy components
    extract(year from date_day) as date_year,
    extract(month from date_day) as date_month,
    extract(day from date_day) as date_day_of_month,
    extract(quarter from date_day) as date_quarter,
    extract(week from date_day) as date_week,
    extract(dayofweek from date_day) as day_of_week,
    extract(dayofyear from date_day) as day_of_year,
    
    -- Formatted date strings
    format_date('%Y-%m-%d', date_day) as date_iso,
    format_date('%Y-%m', date_day) as year_month,
    format_date('%Y-Q%q', date_day) as year_quarter,
    format_date('%Y-W%W', date_day) as year_week,
    
    -- Day name and month name
    format_date('%A', date_day) as day_name,
    format_date('%B', date_day) as month_name,
    
    -- Business logic flags
    case 
      when extract(dayofweek from date_day) in (1, 7) then true 
      else false 
    end as is_weekend,
    
    case 
      when extract(dayofweek from date_day) between 2 and 6 then true 
      else false 
    end as is_weekday,
    
    -- Month boundary indicators (useful for period-over-period analysis)
    case 
      when extract(day from date_day) = 1 then true 
      else false 
    end as is_month_start,
    
    case 
      when date_day = last_day(date_day, month) then true 
      else false 
    end as is_month_end,
    
    -- Quarter boundary indicators
    case 
      when extract(day from date_day) = 1 
        and extract(month from date_day) in (1, 4, 7, 10) 
      then true 
      else false 
    end as is_quarter_start,
    
    case 
      when date_day = last_day(date_day, quarter) then true 
      else false 
    end as is_quarter_end,
    
    -- Year boundary indicators
    case 
      when extract(month from date_day) = 1 and extract(day from date_day) = 1 
      then true 
      else false 
    end as is_year_start,
    
    case 
      when extract(month from date_day) = 12 and extract(day from date_day) = 31 
      then true 
      else false 
    end as is_year_end,
    
    -- Relative date calculations (useful for filtering)
    date_diff(current_date(), date_day, day) as days_from_today,
    date_diff(current_date(), date_day, month) as months_from_today,
    date_diff(current_date(), date_day, year) as years_from_today,
    
    -- Business calendar flags (customize for your organization)
    case 
      when extract(month from date_day) = 12 and extract(day from date_day) = 25 
      then 'Christmas'
      when extract(month from date_day) = 1 and extract(day from date_day) = 1 
      then 'New Year'
      when extract(month from date_day) = 7 and extract(day from date_day) = 4 
      then 'Independence Day'
      when extract(month from date_day) = 11 
        and extract(dayofweek from date_day) = 5  -- Thursday
        and extract(day from date_day) between 22 and 28 
      then 'Thanksgiving'
      -- Add more holidays as needed
      else null
    end as holiday_name,
    
    case 
      when extract(month from date_day) = 12 and extract(day from date_day) = 25 
        or extract(month from date_day) = 1 and extract(day from date_day) = 1 
        or extract(month from date_day) = 7 and extract(day from date_day) = 4 
        or (extract(month from date_day) = 11 
            and extract(dayofweek from date_day) = 5 
            and extract(day from date_day) between 22 and 28)
      then true 
      else false 
    end as is_holiday,
    
    -- Business season classification (customize for your business)
    case 
      when extract(month from date_day) in (12, 1, 2) then 'Winter'
      when extract(month from date_day) in (3, 4, 5) then 'Spring'
      when extract(month from date_day) in (6, 7, 8) then 'Summer'
      when extract(month from date_day) in (9, 10, 11) then 'Fall'
    end as season,
    
    -- Retail calendar periods (customize based on your fiscal calendar)
    case 
      when extract(month from date_day) between 11 and 12 then 'Holiday Season'
      when extract(month from date_day) between 1 and 2 then 'Post-Holiday'
      when extract(month from date_day) between 3 and 5 then 'Spring'
      when extract(month from date_day) between 6 and 8 then 'Summer'
      when extract(month from date_day) between 9 and 10 then 'Back-to-School'
    end as retail_period,
    
    -- Fiscal year calculations (assuming fiscal year starts in April)
    case 
      when extract(month from date_day) >= 4 
      then extract(year from date_day) + 1
      else extract(year from date_day)
    end as fiscal_year,
    
    case 
      when extract(month from date_day) >= 4 
      then extract(month from date_day) - 3
      else extract(month from date_day) + 9
    end as fiscal_month,
    
    case 
      when extract(month from date_day) in (4, 5, 6) then 1
      when extract(month from date_day) in (7, 8, 9) then 2  
      when extract(month from date_day) in (10, 11, 12) then 3
      when extract(month from date_day) in (1, 2, 3) then 4
    end as fiscal_quarter
    
  from date_spine
),

-- Add derived calculations that depend on the enhanced spine
final_time_spine as (
  select 
    *,
    
    -- Prior period calculations for easy period-over-period comparisons
    date_sub(date_day, interval 1 day) as prior_day,
    date_sub(date_day, interval 1 week) as prior_week,
    date_sub(date_day, interval 1 month) as prior_month,
    date_sub(date_day, interval 1 quarter) as prior_quarter,
    date_sub(date_day, interval 1 year) as prior_year,
    
    -- Same day calculations for year-over-year comparisons
    date_sub(date_day, interval 1 year) as same_day_prior_year,
    date_sub(date_day, interval 2 year) as same_day_two_years_prior,
    
    -- Business day calculations (excluding weekends)
    case 
      when is_weekday then 
        row_number() over (
          partition by date_year, date_month 
          order by date_day
        )
      else null 
    end as business_day_of_month,
    
    case 
      when is_weekday then 
        row_number() over (
          partition by fiscal_year, fiscal_quarter 
          order by date_day
        )
      else null 
    end as business_day_of_quarter,
    
    -- First and last business day flags
    case 
      when is_weekday and date_day = (
        select min(date_day) 
        from enhanced_time_spine as inner_spine 
        where inner_spine.date_year = enhanced_time_spine.date_year 
          and inner_spine.date_month = enhanced_time_spine.date_month 
          and inner_spine.is_weekday
      ) then true 
      else false 
    end as is_first_business_day_of_month,
    
    case 
      when is_weekday and date_day = (
        select max(date_day) 
        from enhanced_time_spine as inner_spine 
        where inner_spine.date_year = enhanced_time_spine.date_year 
          and inner_spine.date_month = enhanced_time_spine.date_month 
          and inner_spine.is_weekday
      ) then true 
      else false 
    end as is_last_business_day_of_month
    
  from enhanced_time_spine
)

select * from final_time_spine
order by date_day


-- Usage Examples and Documentation
/*
This time spine model provides comprehensive temporal support for MetricFlow including:

1. BASIC DATE COMPONENTS
   - Standard date hierarchy (year, month, day, quarter, week)
   - Formatted date strings for display
   - Day and month names

2. BUSINESS CALENDAR FEATURES
   - Weekend/weekday flags
   - Holiday identification (customize for your region/business)
   - Seasonal classifications
   - Retail calendar periods

3. FISCAL CALENDAR SUPPORT
   - Configurable fiscal year (currently set to April start)
   - Fiscal months and quarters
   - Business day numbering

4. PERIOD-OVER-PERIOD ANALYSIS
   - Prior period date calculations
   - Same-day prior year comparisons
   - Business day counting and flags

5. PERFORMANCE OPTIMIZATIONS
   - Clustered indexes on frequently queried columns
   - Materialized as table for fast lookups
   - Extended date range (2020-2030) for historical and forecasting needs

CUSTOMIZATION NOTES:
- Adjust start_date and end_date based on your data range
- Modify holiday_name logic for your region/business
- Update fiscal_year calculations if your fiscal year differs
- Add industry-specific seasonal patterns
- Extend date range as needed for long-term planning

METRICFLOW INTEGRATION:
This time spine works automatically with MetricFlow when you:
1. Set it as your time spine model in dbt_project.yml
2. Reference 'date_day' as your time dimension in semantic models
3. Use time-based filters and aggregations in metric definitions

Example dbt_project.yml configuration:
```yaml
semantic-models:
  time-spine:
    model: ref('time_spine')
    primary_time_dimension: date_day
```

Example semantic model time dimension:
```yaml
dimensions:
  - name: order_date
    type: time
    type_params:
      time_granularity: day
    expr: order_date
```
*/