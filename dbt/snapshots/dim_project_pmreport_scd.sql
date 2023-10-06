{%snapshot dim_project_pmreport_scd%}
{{
	config(
		target_schema='snapshots',
		unique_key='prj_pmreportprojects_id',
		strategy='timestamp',
		updated_at='last_modification_time',
        invalidate_hard_deletes=True
	)
}}
	select * from {{ ref(
		'dim_project_pmreport'
		)}}
{%endsnapshot%}
