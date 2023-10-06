{%snapshot dim_employee_skills_scd%}
{{
	config(
		target_schema='snapshots',
		unique_key='prj_user_id',
		strategy='timestamp',
		updated_at='employee_skill_last_modification_time'
	)
}}
	select * from {{ ref(
		'dim_employee_skills'
		)}}
{%endsnapshot%}





