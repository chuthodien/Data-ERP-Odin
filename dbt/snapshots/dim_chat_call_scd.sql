{%snapshot dim_chat_call_scd%}
{{
	config(
		target_schema='snapshots',
		unique_key='user_id_komu',
		strategy='timestamp',
		updated_at='last_modification_time',
        invalidate_hard_deletes=True
	)
}}
	select * from {{ ref(
		'dim_chat_call'
		)}}
{%endsnapshot%}
