{%snapshot dim_employee_absence_scd%}
{{
	config(
		target_schema='snapshots',
		unique_key='tsh_absencedayrequests_id',
		strategy='timestamp',
		updated_at='last_modification_time',
        invalidate_hard_deletes=True
	)
}}
	select * from {{ ref(
		'dim_employee_absence'
		)}}
{%endsnapshot%}
