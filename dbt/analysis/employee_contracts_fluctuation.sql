SELECT *
FROM {{ ref('fct_employee_fluctuation') }} fes
	left join  {{ ref('fct_contract_fluctuation') }} on de.email = fes.employee_email
