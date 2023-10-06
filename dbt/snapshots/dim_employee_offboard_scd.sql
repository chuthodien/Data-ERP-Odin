{%snapshot dim_employee_offboard_scd%}
{{
	config(
		target_schema='snapshots',
		unique_key='hrm_user_id',
		strategy='timestamp',
		updated_at='last_modification_time',
        invalidate_hard_deletes=True
	)
}}
	select * from {{ ref(
		'dim_employee_onboard'
		)}}
{%endsnapshot%}







