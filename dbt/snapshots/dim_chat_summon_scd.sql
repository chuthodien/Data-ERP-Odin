{%snapshot dim_chat_summon_scd%}
{{
	config(
		target_schema='snapshots',
		unique_key='employee_email',
		strategy='timestamp',
		updated_at='last_modification_time',
        invalidate_hard_deletes=True
	)
}}
	select * from {{ ref(
		'dim_chat_summon'
		)}}
{%endsnapshot%}
