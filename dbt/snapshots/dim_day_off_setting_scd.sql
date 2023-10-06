{%snapshot dim_day_off_setting_scd%}
{{
	config(
		target_schema='snapshots',
		unique_key='tsh_dayoffsettings_id',
		strategy='timestamp',
		updated_at='last_modification_time',
        invalidate_hard_deletes=True
	)
}}
	select * from {{ ref(
		'dim_day_off_setting'
		)}}
{%endsnapshot%}
