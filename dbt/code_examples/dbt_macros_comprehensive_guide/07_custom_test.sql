-- Custom test macro for checking null proportion
-- Usage in schema.yml: tests: [not_null_proportion: {at_least: 0.95}]

{% test not_null_proportion(model, column_name, at_least=0.95) %}
    select *
    from (
        select
            avg(case when {{ column_name }} is not null then 1.0 else 0.0 end) as not_null_proportion
        from {{ model }}
    ) validation
    where not_null_proportion < {{ at_least }}
{% endtest %}

-- Example usage in schema.yml:
-- models:
--   - name: customers
--     columns:
--       - name: email
--         tests:
--           - not_null_proportion:
--               at_least: 0.98