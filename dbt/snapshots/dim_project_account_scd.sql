{%snapshot dim_project_account_scd%}
{{
	config(
		target_schema='snapshots',
		unique_key='prj_timesheetprojects_id',       
		strategy='timestamp',
		updated_at='last_modification_time',
        invalidate_hard_deletes=True
	)
}}
	select * from {{ ref(
		'dim_project_account'
		)}}
{%endsnapshot%}



