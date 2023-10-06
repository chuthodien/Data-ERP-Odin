SELECT 
	*
FROM {{ ref('fct_employee_checkin_daily') }} fecd
LEFT JOIN  {{ ref('fct_employee')}} fe
	ON 
		fecd.employee_email = fe.email
