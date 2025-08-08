-- Performance Monitoring Macros for dbt
-- Comprehensive performance tracking and alerting system

-- ============================================================================
-- CORE PERFORMANCE TRACKING MACROS
-- ============================================================================

{% macro log_model_performance() %}
    {% if execute %}
        {% set performance_table_sql %}
            create table if not exists {{ target.database }}.analytics.dbt_performance_log (
                run_id varchar(255),
                model_name varchar(255),
                execution_time_seconds float,
                rows_affected bigint,
                bytes_processed bigint,
                warehouse_used varchar(100),
                warehouse_size varchar(50),
                target_name varchar(100),
                run_timestamp timestamp,
                git_sha varchar(255),
                dbt_version varchar(50),
                thread_count int,
                materialization varchar(50),
                incremental_strategy varchar(50)
            );
        {% endset %}
        
        {% do run_query(performance_table_sql) %}
        
        {% for result in results %}
            {% if result.node.resource_type == 'model' %}
                {% set insert_sql %}
                    insert into {{ target.database }}.analytics.dbt_performance_log values (
                        '{{ invocation_id }}',
                        '{{ result.node.name }}',
                        {{ result.timing[0].completed_at | as_timestamp - result.timing[0].started_at | as_timestamp }},
                        {{ result.adapter_response.rows_affected | default(0) }},
                        {{ result.adapter_response.bytes_processed | default(0) }},
                        '{{ target.warehouse }}',
                        '{{ var("warehouse_size", "unknown") }}',
                        '{{ target.name }}',
                        current_timestamp(),
                        '{{ env_var("GIT_SHA", "unknown") }}',
                        '{{ dbt_version }}',
                        {{ target.threads }},
                        '{{ result.node.config.materialized }}',
                        '{{ result.node.config.incremental_strategy | default("n/a") }}'
                    );
                {% endset %}
                {% do run_query(insert_sql) %}
            {% endif %}
        {% endfor %}
    {% endif %}
{% endmacro %}


-- ============================================================================
-- PERFORMANCE ANALYSIS MACROS
-- ============================================================================

{% macro analyze_model_performance(days_back=7) %}
    {% set analysis_query %}
        with model_performance as (
            select 
                model_name,
                materialization,
                target_name,
                
                -- Execution metrics
                count(*) as total_runs,
                avg(execution_time_seconds) as avg_runtime_seconds,
                max(execution_time_seconds) as max_runtime_seconds,
                min(execution_time_seconds) as min_runtime_seconds,
                stddev(execution_time_seconds) as runtime_stddev,
                
                -- Data volume metrics
                avg(rows_affected) as avg_rows_processed,
                max(rows_affected) as max_rows_processed,
                sum(rows_affected) as total_rows_processed,
                avg(bytes_processed) as avg_bytes_processed,
                
                -- Efficiency metrics
                avg(execution_time_seconds / nullif(rows_affected, 0)) as avg_seconds_per_row,
                avg(bytes_processed / nullif(execution_time_seconds, 0)) as avg_bytes_per_second,
                
                -- Cost estimation (approximate)
                sum(execution_time_seconds / 3600.0) * 2.50 as estimated_total_cost_usd,
                
                -- Trend analysis
                min(run_timestamp) as first_run,
                max(run_timestamp) as last_run
                
            from {{ target.database }}.analytics.dbt_performance_log
            where run_timestamp >= current_date - {{ days_back }}
            group by model_name, materialization, target_name
        ),
        
        performance_rankings as (
            select 
                *,
                -- Rank by different performance aspects
                row_number() over (order by avg_runtime_seconds desc) as slowest_rank,
                row_number() over (order by estimated_total_cost_usd desc) as most_expensive_rank,
                row_number() over (order by total_rows_processed desc) as largest_data_rank,
                
                -- Performance categories
                case 
                    when avg_runtime_seconds > 300 then 'very_slow'
                    when avg_runtime_seconds > 120 then 'slow' 
                    when avg_runtime_seconds > 30 then 'moderate'
                    else 'fast'
                end as performance_category,
                
                -- Optimization recommendations
                case 
                    when materialization = 'view' and avg_runtime_seconds > 60 then 'Consider table materialization'
                    when materialization = 'table' and max_runtime_seconds < 10 and total_runs > 10 then 'Consider view materialization'
                    when materialization = 'table' and max_rows_processed > 10000000 then 'Consider incremental strategy'
                    when avg_seconds_per_row > 0.01 then 'Optimize query performance'
                    when runtime_stddev / avg_runtime_seconds > 0.5 then 'Inconsistent performance - investigate'
                    else 'Performance acceptable'
                end as recommendation
                
            from model_performance
        )
        
        select * from performance_rankings
        order by avg_runtime_seconds desc;
    {% endset %}
    
    {{ return(run_query(analysis_query)) }}
{% endmacro %}


-- ============================================================================
-- PERFORMANCE ALERTING MACROS
-- ============================================================================

{% macro check_performance_anomalies(threshold_multiplier=2.0, min_runtime=60) %}
    {% if execute %}
        {% set anomaly_check %}
            with recent_performance as (
                select 
                    model_name,
                    execution_time_seconds,
                    run_timestamp,
                    
                    -- Calculate rolling average (excluding current run)
                    avg(execution_time_seconds) over (
                        partition by model_name 
                        order by run_timestamp 
                        rows between 10 preceding and 1 preceding
                    ) as avg_historical_runtime,
                    
                    -- Calculate performance ratio
                    execution_time_seconds / nullif(
                        avg(execution_time_seconds) over (
                            partition by model_name 
                            order by run_timestamp 
                            rows between 10 preceding and 1 preceding
                        ), 0
                    ) as performance_ratio
                    
                from {{ target.database }}.analytics.dbt_performance_log
                where run_timestamp >= current_timestamp - interval '2 hours'
                  and model_name != 'dbt_performance_log'  -- Exclude self
            ),
            
            anomalies as (
                select 
                    model_name,
                    execution_time_seconds,
                    avg_historical_runtime,
                    performance_ratio,
                    run_timestamp,
                    
                    case 
                        when execution_time_seconds >= {{ min_runtime }} 
                             and performance_ratio >= {{ threshold_multiplier * 2 }} then 'critical'
                        when execution_time_seconds >= {{ min_runtime }} 
                             and performance_ratio >= {{ threshold_multiplier }} then 'warning'
                        else 'normal'
                    end as alert_level,
                    
                    -- Generate alert message
                    'Model ' || model_name || ' took ' || round(execution_time_seconds, 1) || 's' ||
                    ' (normal: ' || round(avg_historical_runtime, 1) || 's, ' || 
                    round(performance_ratio, 2) || 'x slower)' as alert_message
                    
                from recent_performance
                where avg_historical_runtime is not null
            )
            
            select * from anomalies 
            where alert_level in ('critical', 'warning')
            order by performance_ratio desc;
        {% endset %}
        
        {% set results = run_query(anomaly_check) %}
        
        {% if results.rows %}
            {{ log("🔍 Performance Anomalies Detected:", info=true) }}
            {% for row in results.rows %}
                {% if row[5] == 'critical' %}
                    {{ log("🚨 CRITICAL: " ~ row[6], info=true) }}
                {% else %}
                    {{ log("⚠️  WARNING: " ~ row[6], info=true) }}
                {% endif %}
            {% endfor %}
        {% else %}
            {{ log("✅ No performance anomalies detected", info=true) }}
        {% endif %}
    {% endif %}
{% endmacro %}


-- ============================================================================
-- COST TRACKING MACROS
-- ============================================================================

{% macro track_warehouse_costs() %}
    {% if target.type == 'snowflake' %}
        {% set cost_tracking %}
            with warehouse_usage as (
                select 
                    warehouse_name,
                    start_time,
                    end_time,
                    credits_used,
                    
                    -- Calculate cost at $2 per credit (adjust as needed)
                    credits_used * 2.0 as cost_usd,
                    
                    -- Time-based analysis
                    extract(hour from start_time) as usage_hour,
                    extract(dayofweek from start_time) as day_of_week,
                    date_trunc('day', start_time) as usage_date
                    
                from table(information_schema.warehouse_metering_history(
                    date_range_start => dateadd('day', -7, current_date()),
                    date_range_end => current_date()
                ))
                where warehouse_name = '{{ target.warehouse }}'
            ),
            
            cost_summary as (
                select 
                    warehouse_name,
                    usage_date,
                    sum(credits_used) as daily_credits,
                    sum(cost_usd) as daily_cost_usd,
                    
                    -- Peak usage analysis
                    max(credits_used) as peak_hourly_credits,
                    avg(credits_used) as avg_hourly_credits,
                    
                    -- Efficiency metrics
                    sum(cost_usd) / nullif(sum(credits_used), 0) as cost_per_credit
                    
                from warehouse_usage
                group by warehouse_name, usage_date
            ),
            
            cost_recommendations as (
                select 
                    *,
                    case 
                        when daily_cost_usd > 100 then 'Review large warehouse usage'
                        when peak_hourly_credits / avg_hourly_credits > 3 then 'Consider auto-scaling'
                        when daily_credits < 1 then 'Consider smaller warehouse'
                        else 'Usage appears optimal'
                    end as recommendation
                    
                from cost_summary
            )
            
            select * from cost_recommendations
            order by daily_cost_usd desc;
        {% endset %}
        
        {{ return(run_query(cost_tracking)) }}
    {% endif %}
{% endmacro %}


-- ============================================================================
-- INCREMENTAL PERFORMANCE MONITORING
-- ============================================================================

{% macro monitor_incremental_efficiency() %}
    {% if is_incremental() %}
        {% set efficiency_check %}
            with incremental_analysis as (
                select 
                    count(*) as records_processed_this_run
                from {{ this }}
                where _dbt_updated_at >= '{{ run_started_at }}'
            ),
            
            total_records as (
                select count(*) as total_records
                from {{ this }}
            ),
            
            efficiency_metrics as (
                select 
                    i.records_processed_this_run,
                    t.total_records,
                    (i.records_processed_this_run::float / t.total_records::float) * 100 as incremental_percentage,
                    
                    case 
                        when (i.records_processed_this_run::float / t.total_records::float) > 0.5 then 'inefficient'
                        when (i.records_processed_this_run::float / t.total_records::float) > 0.2 then 'moderate'
                        else 'efficient'
                    end as efficiency_rating
                    
                from incremental_analysis i
                cross join total_records t
            )
            
            select * from efficiency_metrics;
        {% endset %}
        
        {% set results = run_query(efficiency_check) %}
        {% if execute and results.rows %}
            {% set pct = results.columns[2].values()[0] %}
            {% set rating = results.columns[3].values()[0] %}
            
            {{ log("📊 Incremental Efficiency: " ~ pct ~ "% of data processed", info=true) }}
            
            {% if rating == 'inefficient' %}
                {{ log("⚠️  WARNING: Incremental processing >50% of data. Consider optimization or full refresh.", info=true) }}
            {% elif rating == 'moderate' %}
                {{ log("🔍 INFO: Incremental processing 20-50% of data. Monitor for optimization opportunities.", info=true) }}
            {% else %}
                {{ log("✅ Incremental processing is efficient (<20% of data).", info=true) }}
            {% endif %}
        {% endif %}
    {% endif %}
{% endmacro %}


-- ============================================================================
-- QUERY PLAN ANALYSIS (SNOWFLAKE)
-- ============================================================================

{% macro analyze_expensive_queries(hours_back=24, min_execution_seconds=30) %}
    {% if target.type == 'snowflake' %}
        {% set query_analysis %}
            select 
                query_id,
                query_text,
                database_name,
                schema_name,
                warehouse_name,
                
                -- Execution metrics
                total_elapsed_time / 1000 as execution_seconds,
                compilation_time / 1000 as compilation_seconds,
                execution_time / 1000 as pure_execution_seconds,
                
                -- Resource usage
                bytes_scanned,
                bytes_written,
                bytes_deleted,
                bytes_spilled_to_local_storage,
                bytes_spilled_to_remote_storage,
                
                -- Performance indicators
                case 
                    when bytes_spilled_to_remote_storage > 0 then 'memory_constrained'
                    when compilation_seconds > execution_seconds * 0.5 then 'compilation_heavy'
                    when bytes_scanned / execution_seconds > 1000000000 then 'io_intensive'
                    else 'compute_intensive'
                end as performance_profile,
                
                -- Optimization suggestions
                case 
                    when query_text ilike '%order by%' and query_text not ilike '%limit%' then 'Add LIMIT clause'
                    when query_text ilike '%distinct%' and execution_seconds > 60 then 'Review DISTINCT necessity'
                    when query_text ilike '%cross join%' then 'Review cross join - use explicit joins'
                    when bytes_spilled_to_remote_storage > 0 then 'Increase warehouse size'
                    when compilation_seconds > 10 then 'Simplify query structure'
                    else 'Performance acceptable'
                end as optimization_suggestion
                
            from table(information_schema.query_history_by_warehouse('{{ target.warehouse }}'))
            where start_time >= dateadd(hour, -{{ hours_back }}, current_timestamp)
              and execution_seconds >= {{ min_execution_seconds }}
              and query_text ilike '%dbt%'  -- Focus on dbt queries
              and query_type = 'SELECT'
            
            order by execution_seconds desc
            limit 20;
        {% endset %}
        
        {{ return(run_query(query_analysis)) }}
    {% endif %}
{% endmacro %}


-- ============================================================================
-- COMPREHENSIVE PERFORMANCE DASHBOARD
-- ============================================================================

{% macro create_performance_dashboard() %}
    {% set dashboard_sql %}
        create or replace table {{ target.database }}.analytics.dbt_performance_dashboard as
        
        with daily_summary as (
            select 
                date_trunc('day', run_timestamp) as run_date,
                target_name,
                
                -- Volume metrics
                count(distinct model_name) as models_run,
                count(*) as total_runs,
                sum(rows_affected) as total_rows_processed,
                
                -- Performance metrics
                avg(execution_time_seconds) as avg_runtime,
                max(execution_time_seconds) as max_runtime,
                sum(execution_time_seconds) as total_runtime,
                
                -- Cost metrics
                sum(execution_time_seconds / 3600.0) * 2.50 as estimated_daily_cost,
                
                -- Quality metrics
                count(case when execution_time_seconds > 300 then 1 end) as slow_models_count,
                count(case when execution_time_seconds < 5 then 1 end) as fast_models_count
                
            from {{ target.database }}.analytics.dbt_performance_log
            where run_timestamp >= current_date - 30
            group by date_trunc('day', run_timestamp), target_name
        ),
        
        trend_analysis as (
            select 
                *,
                -- 7-day moving averages
                avg(avg_runtime) over (
                    partition by target_name 
                    order by run_date 
                    rows between 6 preceding and current row
                ) as avg_runtime_7d,
                
                avg(estimated_daily_cost) over (
                    partition by target_name 
                    order by run_date 
                    rows between 6 preceding and current row
                ) as avg_cost_7d,
                
                -- Performance trends
                case 
                    when avg_runtime > avg(avg_runtime) over (partition by target_name) * 1.2 then 'degrading'
                    when avg_runtime < avg(avg_runtime) over (partition by target_name) * 0.8 then 'improving'
                    else 'stable'
                end as performance_trend
                
            from daily_summary
        )
        
        select * from trend_analysis
        order by run_date desc, target_name;
    {% endset %}
    
    {% do run_query(dashboard_sql) %}
    {{ log("✅ Performance dashboard created at " ~ target.database ~ ".analytics.dbt_performance_dashboard", info=true) }}
{% endmacro %}


-- ============================================================================
-- USAGE EXAMPLES IN dbt_project.yml
-- ============================================================================

-- Add to dbt_project.yml:
-- on-run-start:
--   - "{{ check_performance_anomalies() }}"
--
-- on-run-end:
--   - "{{ log_model_performance() }}"
--   - "{{ create_performance_dashboard() }}"
--
-- Post-hooks for specific models:
-- {{ config(
--     post_hook=[
--       "{{ monitor_incremental_efficiency() }}",
--       "{{ track_warehouse_costs() }}"
--     ]
-- ) }}