{%snapshot dim_chat_message_scd%}
{{
	config(
		target_schema='snapshots',
		unique_key='komu_msg_id',
		strategy='timestamp',
		updated_at='created_time'
		
	)
}}
	select * from {{ ref(
		'dim_chat_message'
		)}}
{%endsnapshot%}