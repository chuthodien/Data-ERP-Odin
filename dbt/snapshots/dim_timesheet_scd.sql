{%snapshot dim_timesheet_scd%}
{{
	config(
		target_schema='snapshots',
		unique_key='tsh_mytimesheet_id',
		strategy='timestamp',
		updated_at='last_modification_time',
        invalidate_hard_deletes=True
	)
}}
	select * from {{ ref(
		'dim_timesheet'
		)}}
{%endsnapshot%}
