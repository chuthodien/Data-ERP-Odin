{%snapshot dim_talent_cveducations_scd%}
{{
	config(
		target_schema='snapshots',
		unique_key='tal_cv_candidate_id',
		strategy='timestamp',
		updated_at='cv_creation_time'
	)
}}
	select * from {{ ref(
		'dim_talent_cveducations'
		)}}
{%endsnapshot%}
