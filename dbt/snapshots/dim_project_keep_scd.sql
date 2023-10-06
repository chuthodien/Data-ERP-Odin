{%snapshot dim_project_keep_scd%}
{{
	config(
		target_schema='snapshots',
		unique_key='tsh_project_id',
		strategy='timestamp',
		updated_at='last_modification_time',
        invalidate_hard_deletes=True
	)
}}
	select * from {{ ref(
		'dim_project_keep'
		)}}
{%endsnapshot%}
