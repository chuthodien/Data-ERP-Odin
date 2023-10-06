{%snapshot dim_client_scd%}
{{
	config(
		target_schema='snapshots',
		unique_key='prj_clients_id',
		strategy='timestamp',
		updated_at='client_last_modification_time'
	)
}}
	select * from {{ ref(
		'dim_client'
		)}}
{%endsnapshot%}