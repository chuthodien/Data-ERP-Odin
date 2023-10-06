{{ config(
    materialized = 'table',
    schema = 'stage'
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ ref(
            'base_prj_projects'
        ) }}
),
stg_employee AS (
    SELECT
        *
    FROM
        {{ ref('stg_employee') }}
),
data_mapped AS (
    SELECT
        "prj_project_id",
        "project_code",
        "project_name",
        "pm_user_id",
        "project_status",
        "end_time",
        "client_id",
        "is_charge",
        s."is_deleted",
        "start_time",
        "charge_type",
        "currency_id",
        "evaluation",
        "project_type",
        s."creation_time",
        s."deletion_time",
        "new_knowledge",
        "other_problems",
        "technology_used",
        "brief_description",
        "detail_description",
        "technical_problems",
        s."last_modification_time",
        se.email AS pm_email
    FROM
        source s
    LEFT JOIN stg_employee se
        ON s.pm_user_id = se.prj_user_id
)
SELECT
    *
FROM
    data_mapped
