{%snapshot dim_talent_requests_scd%}
{{
	config(
		target_schema='snapshots',
		unique_key='request_id',
		strategy='timestamp',
		updated_at='last_modification_time',
        invalidate_hard_deletes=True
	)
}}
	select * from {{ ref(
		'dim_talent_requests'
		)}}
{%endsnapshot%}
