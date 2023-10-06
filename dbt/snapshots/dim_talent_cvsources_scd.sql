{%snapshot dim_talent_cvsources_scd%}
{{
	config(
		target_schema='snapshots',
		unique_key='tal_cv_candidate_id',
		strategy='timestamp',
		updated_at='last_modification_time',
        invalidate_hard_deletes=True
	)
}}
	select * from {{ ref(
		'dim_talent_cvsources'
		)}}
{%endsnapshot%}
