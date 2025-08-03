-- Email validation macro
-- Usage: {{ validate_email('email_column') }} as email_status

{% macro validate_email(email_column) %}
    case
        when {{ email_column }} is null then 'missing'
        when length({{ email_column }}) = 0 then 'empty'
        when {{ email_column }} not like '%@%' then 'no_at_symbol'
        when {{ email_column }} not like '%.%' then 'no_domain'
        when length({{ email_column }}) > 254 then 'too_long'
        when {{ email_column }} like '%@%@%' then 'multiple_at_symbols'
        else 'valid'
    end
{% endmacro %}

-- Example usage in a model:
-- select 
--     customer_id,
--     email,
--     {{ validate_email('email') }} as email_validation_status,
--     case when {{ validate_email('email') }} = 'valid' then true else false end as is_valid_email
-- from {{ source('raw', 'customers') }}