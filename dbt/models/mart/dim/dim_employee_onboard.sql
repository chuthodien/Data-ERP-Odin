{{ config(
    schema = 'mart',
    materialized = "table"
) }}

SELECT
    *,
    email AS employee_email,
    {{ add_date_at('onboard_date') }}
FROM
    {{ ref('dim_employee') }}
WHERE
    onboard_date IS NOT NULL
    AND is_deleted IS FALSE
