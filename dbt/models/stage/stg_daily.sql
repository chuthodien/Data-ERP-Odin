{{ config(
    schema = 'stage',
    materialized = "table"
) }}

WITH prj_users AS (
    SELECT
        *
    FROM
        {{ ref(
            'base_prj_abpusers'
        ) }}
),
prj AS (

    SELECT
        *
    FROM
        {{ ref(
            'base_prj_projects'
        ) }}
),
projectuser AS (

    SELECT
        *
    FROM
        {{ ref(
            'base_prj_projectusers'
        ) }}
)

SELECT
    dailies.komu_user_id,
    channel_id,
    daily,
    user_email,
    pu.prj_user_id,
    pu.username,
    pr.project_name,
    pr.project_status,
    pr.pm_user_id,
    dailies.created_at
FROM
    {{ ref('base_komu_dailies') }} as dailies
    LEFT JOIN prj_users pu
    ON dailies.user_email = pu.email
    LEFT JOIN projectuser pru
    ON pru.prj_user_id = pu.prj_user_id
    LEFT JOIN prj pr
    ON pr.prj_project_id = pru.project_id
