{%snapshot dim_bill_projects_scd%}
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
		'dim_bill_projects'
		)}}
{%endsnapshot%}
