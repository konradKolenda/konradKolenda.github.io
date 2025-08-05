-- Performance and deployment monitoring macros
-- macros/monitoring.sql

-- Log deployment audit information
{% macro log_deployment_audit() %}
  {% if execute %}
    {% set audit_query %}
      insert into {{ target.database }}.audit.deployment_log
      select
        current_timestamp() as deployment_timestamp,
        '{{ invocation_id }}' as invocation_id,
        '{{ target.name }}' as target_environment,
        '{{ env_var("GITHUB_ACTOR", "unknown") }}' as deployed_by,
        '{{ env_var("GITHUB_SHA", "unknown") }}' as commit_sha,
        '{{ env_var("GITHUB_REF", "unknown") }}' as branch_ref,
        '{{ results | selectattr("status", "equalto", "success") | list | length }}' as successful_models,
        '{{ results | selectattr("status", "equalto", "error") | list | length }}' as failed_models,
        '{{ run_started_at }}' as run_started_at
    {% endset %}
    
    {% do run_query(audit_query) %}
  {% endif %}
{% endmacro %}

-- Log model performance metrics
{% macro log_model_performance() %}
  {% if execute and results %}
    {% for result in results %}
      {% if result.timing %}
        {% set performance_query %}
          insert into {{ target.database }}.audit.model_performance_log
          select
            current_timestamp() as logged_at,
            '{{ invocation_id }}' as invocation_id,
            '{{ result.node.name }}' as model_name,
            '{{ result.node.resource_type }}' as resource_type,
            '{{ target.name }}' as target_environment,
            {{ result.timing | sum }} as execution_time_seconds,
            '{{ result.status }}' as status,
            {% if result.adapter_response %}
              '{{ result.adapter_response.get("bytes_processed", 0) }}' as bytes_processed,
              '{{ result.adapter_response.get("rows_affected", 0) }}' as rows_affected
            {% else %}
              0 as bytes_processed,
              0 as rows_affected
            {% endif %}
        {% endset %}
        
        {% do run_query(performance_query) %}
      {% endif %}
    {% endfor %}
  {% endif %}
{% endmacro %}

-- Alert on test failures via webhook
{% macro alert_on_test_failure() %}
  {% if execute %}
    {% set failed_tests_query %}
      select 
        test_name,
        model_name,
        status,
        fail_count
      from (
        {{ results | selectattr("status", "equalto", "fail") | list }}
      ) as failed_tests
      where fail_count > 0
    {% endset %}
    
    {% set results = run_query(failed_tests_query) %}
    
    {% if results.rows %}
      {% set webhook_url = var('slack_webhook_url', '') %}
      {% if webhook_url %}
        {% set message = {
          'text': 'dbt Test Failures Detected in ' ~ target.name,
          'attachments': [
            {
              'color': 'danger',
              'fields': [
                {
                  'title': 'Environment',
                  'value': target.name,
                  'short': true
                },
                {
                  'title': 'Failed Tests',
                  'value': results.rows | length | string,
                  'short': true
                },
                {
                  'title': 'Run ID',
                  'value': env_var('GITHUB_RUN_ID', invocation_id),
                  'short': true
                }
              ],
              'text': 'Failed tests:\n' ~ (results.rows | map(attribute=0) | join('\n'))
            }
          ]
        } %}
        
        -- Note: In real implementation, you would use a custom Python script
        -- or external service to actually send the webhook
        {{ log("Would send Slack alert: " ~ message, info=true) }}
      {% endif %}
    {% endif %}
  {% endif %}
{% endmacro %}

-- Get current deployment slot for blue-green deployments
{% macro get_current_slot() %}
  {% set query %}
    select current_slot 
    from {{ target.database }}.config.deployment_slots 
    where is_active = true
    limit 1
  {% endset %}
  
  {% if execute %}
    {% set results = run_query(query) %}
    {% if results.rows %}
      {{ return(results.rows[0][0]) }}
    {% else %}
      {{ return('blue') }}  -- Default to blue if no active slot found
    {% endif %}
  {% endif %}
{% endmacro %}

-- Switch deployment slot for blue-green deployments
{% macro switch_slot(slot) %}
  {% set switch_query %}
    -- Update deployment slot configuration
    update {{ target.database }}.config.deployment_slots
    set is_active = case when slot_name = '{{ slot }}' then true else false end,
        updated_at = current_timestamp(),
        updated_by = '{{ env_var("GITHUB_ACTOR", "system") }}'
  {% endset %}
  
  {% set create_aliases_query %}
    -- Create or replace aliases/views pointing to new slot
    {% for model in graph.nodes.values() %}
      {% if model.resource_type == 'model' %}
        create or replace view {{ target.database }}.{{ target.schema }}.{{ model.name }}
        as select * from {{ target.database }}.{{ target.schema }}_{{ slot }}.{{ model.name }};
      {% endif %}
    {% endfor %}
  {% endset %}
  
  {% if execute %}
    {% do run_query(switch_query) %}
    {% do run_query(create_aliases_query) %}
    {{ log("Switched deployment to " ~ slot ~ " slot", info=true) }}
  {% endif %}
{% endmacro %}

-- Cleanup deployment slot
{% macro cleanup_slot(slot) %}
  {% set cleanup_query %}
    drop schema if exists {{ target.database }}.{{ target.schema }}_{{ slot }} cascade
  {% endset %}
  
  {% if execute %}
    {% do run_query(cleanup_query) %}
    {{ log("Cleaned up " ~ slot ~ " slot", info=true) }}
  {% endif %}
{% endmacro %}

-- Drop schema for PR environment cleanup
{% macro drop_schema(schema_name) %}
  {% set drop_query %}
    drop schema if exists {{ target.database }}.{{ schema_name }} cascade
  {% endset %}
  
  {% if execute %}
    {% do run_query(drop_query) %}
    {{ log("Dropped schema: " ~ schema_name, info=true) }}
  {% endif %}
{% endmacro %}