{%snapshot dim_talent_email_templates_scd%}
{{
	config(
		target_schema='snapshots',
		unique_key='tal_email_template_id',
		strategy='timestamp',
		updated_at='last_modification_time',
        invalidate_hard_deletes=True
	)
}}
	select * from {{ ref(
		'dim_talent_email_templates'
		)}}
{%endsnapshot%}
