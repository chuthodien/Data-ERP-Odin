{%snapshot dim_deal_scd%}
{{
	config(
		target_schema='snapshots',
		unique_key='crm_deal_id',
		strategy='timestamp',
		updated_at='detal_last_modification_time'
	)
}}
	select * from {{ ref(
		'dim_deal'
		)}}
{%endsnapshot%}