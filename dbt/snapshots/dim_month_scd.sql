{%snapshot dim_month_scd%}
{{
	config(
		target_schema='snapshots',
		unique_key='date_day',
		strategy='timestamp',
		updated_at='date_month'
	)
}}
	select * from {{ ref(
		'dim_month'
		)}}
{%endsnapshot%}



