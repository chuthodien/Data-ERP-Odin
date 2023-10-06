{{ config(
    materialized = 'table',
    schema = 'stage'
) }}

WITH source AS(

    SELECT
        *
    FROM
        {{ ref('base_prj_projectusers') }}
    WHERE
        employee_project_is_deleted IS FALSE
),
data_type_rename_conversion AS(
    SELECT
        prj_projectuser_id AS project_user_id,
        project_user_note,
        is_pool AS is_temp,
        project_user_status,
        prj_user_id,
        is_expense,
        project_id,
        user_start_time,
        pm_report_id,
        project_role,
        is_future_active,
        resource_request_id,
        allocate_percentage,
        employee_project_creation_time,
        employee_project_deletion_time,
        employee_project_last_modification_time
    FROM
        source
)
SELECT
    *
FROM
    data_type_rename_conversion
