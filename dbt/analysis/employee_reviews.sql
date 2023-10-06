SELECT
    *
FROM {{ref('dim_employee_reviews') }} der
LEFT JOIN {{ref('dim_employee')}} de
    ON de.email = der.employee_email