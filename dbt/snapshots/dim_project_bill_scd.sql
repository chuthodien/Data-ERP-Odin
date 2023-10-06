{%snapshot dim_project_bill_scd%}
{{
	config(
		target_schema='snapshots',
		unique_key='prj_timesheetprojectbills_id',
		strategy='timestamp',
		updated_at='last_modification_time',
        invalidate_hard_deletes=True
	)
}}
	select * from {{ ref(
		'dim_project_bill'
		)}}
{%endsnapshot%}

