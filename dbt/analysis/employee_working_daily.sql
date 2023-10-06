SELECT 
	*
FROM {{ ref('fct_employee_working_daily') }} fewd
LEFT JOIN  {{ ref('fct_employee')}} fe
	ON 
		fewd.employee_email = fe.email
