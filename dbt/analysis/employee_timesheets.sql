SELECT 
	*
FROM {{ ref('dim_timesheet') }} dt
LEFT JOIN  {{ ref('dim_employee')}} de
	ON 
		dt.employee_email = de.email
