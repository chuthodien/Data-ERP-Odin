{%snapshot dim_employee_level_scd%}
{{
	config(
		target_schema='snapshots',
		unique_key='employee_email',
		strategy='timestamp',
		updated_at='date_at',
        invalidate_hard_deletes=True
	)
}}
	select * from {{ ref(
		'dim_employee_level'
		)}}
{%endsnapshot%}
