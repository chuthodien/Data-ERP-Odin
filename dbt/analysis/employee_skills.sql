SELECT *
FROM {{ ref('fct_employee_skills') }} fes
	left join  {{ ref('fct_employee') }} on de.email = fes.employee_email
