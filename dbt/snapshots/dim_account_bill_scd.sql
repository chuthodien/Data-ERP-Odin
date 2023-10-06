{%snapshot dim_account_bill_scd%}
{{
	config(
		target_schema='snapshots',
		unique_key='prj_user_id',
		strategy='timestamp',
		updated_at='last_modification_time',
        invalidate_hard_deletes=True
	)
}}
	select * from {{ ref(
		'dim_account_bill'
		)}}
{%endsnapshot%}
