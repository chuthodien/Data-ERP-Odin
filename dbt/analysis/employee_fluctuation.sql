select
	*
from
	{{ ref('fct_employee_fluctuation') }} fef
	left join  {{ ref('fct_employee') }}  fe on fe.email = fef.employee_email
