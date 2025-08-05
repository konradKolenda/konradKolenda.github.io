-- Custom generic test for email format validation
-- macros/test_email_format.sql

{% macro test_email_format(model, column_name) %}

  select count(*)
  from {{ model }}
  where {{ column_name }} is not null
    and not regexp_like({{ column_name }}, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$')

{% endmacro %}

-- Usage in schema.yml:
-- models:
--   - name: customers
--     columns:
--       - name: email
--         tests:
--           - email_format