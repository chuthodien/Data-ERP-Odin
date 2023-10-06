{% snapshot dim_employee_project_scd %}

{{
    config(
      target_schema='snapshots',
      unique_key='prj_projectuser_id',
      strategy='timestamp',
      updated_at='employee_project_last_modification_time',
    )
}}

    select * from {{ ref('dim_employee_project') }}
    
{% endsnapshot %}
