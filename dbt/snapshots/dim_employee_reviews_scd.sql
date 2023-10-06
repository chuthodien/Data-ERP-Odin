{%snapshot dim_employee_reviews_scd%}
{{
	config(
		target_schema='snapshots',
		unique_key='employee_email',
		strategy='timestamp',
		updated_at='date_at'
	)
}}
	select * from {{ ref(
		'dim_employee_reviews'
		)}}
{%endsnapshot%}





