{%snapshot dim_resource_bill_scd%}
{{
	config(
		target_schema='snapshots',
		unique_key='prj_project_id',
		strategy='timestamp',
		updated_at='last_modification_time',
        invalidate_hard_deletes=True
	)
}}
	select * from {{ ref(
		'dim_resource_bill'
		)}}
{%endsnapshot%}
