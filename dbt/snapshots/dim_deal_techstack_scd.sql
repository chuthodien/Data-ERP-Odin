{%snapshot dim_deal_techstack_scd%}
{{
	config(
		target_schema='snapshots',
		unique_key='crm_deal_id',
		strategy='timestamp',
		updated_at='detal_last_modification_time'
	)
}}
	select * from {{ ref(
		'dim_deal_techstack'
		)}}
{%endsnapshot%}