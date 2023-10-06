SELECT 
	*
FROM {{ ref('dim_employee_absence') }} dt
LEFT JOIN  {{ ref('dim_employee')}} de
	ON 
		dt.employee_email = de.email
