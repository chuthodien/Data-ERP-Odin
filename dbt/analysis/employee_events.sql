SELECT 
    *
FROM {{ref('dim_employee_events')}} dee
LEFT JOIN {{ref('dim_employee')}} de
ON de.email = dee.employee_email