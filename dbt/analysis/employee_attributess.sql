with q as (
select
	email,
	json_build_object(
        'Name', de.fullname
        ,'Email', de.email
        ,'Type', de.employee_type_name
        ,'Job position', de.job_position_name
        ,'Branch', de.branch_name
        ,'Level', de.employee_level_name
        ,'Status', de.employee_status_name
        ,'Manager', de.manager_email
        ,'Gender', de.gender_name
        ,'Date of birth', de.birthday
        ,'Phonenumber', de.phone
        ,'Rating', de.star_rate
        ,'Address',de.address
        ,'Note', de.pool_note
    ) as data
from
		{{ref('dim_employee')}} de 
)
select
	q.email,
	d.key,
	d.value
from
	q
join json_each_text(q.data) d on
	true