{%snapshot dim_timekeepings_scd%}
{{
	config(
		target_schema='snapshots',
		unique_key='employee_email',
		strategy='timestamp',
		updated_at='creation_time'
	)
}}
	select * from {{ ref(
		'dim_timekeepings'
		)}}
{%endsnapshot%}
