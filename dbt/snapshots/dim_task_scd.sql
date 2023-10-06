{%snapshot dim_task_scd%}
{{
	config(
		target_schema='snapshots',
		unique_key='tsh_task_id',
		strategy='timestamp',
		updated_at='last_modification_time',
        invalidate_hard_deletes=True
	)
}}
	select * from {{ ref(
		'dim_task'
		)}}
{%endsnapshot%}
