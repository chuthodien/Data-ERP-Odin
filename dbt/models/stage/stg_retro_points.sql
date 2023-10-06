{{ config(
    materialized = 'table',
    schema = 'stage'
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ ref(
            'base_tsh_retroresults'
        ) }}
    WHERE is_deleted = false
),
base_tsh_retros AS (
    SELECT
        *
    FROM
        {{ ref('base_tsh_retros') }}
    WHERE is_deleted = false
),
project_tsh AS (
    SELECT 
        *
    FROM 
        {{ref('base_tsh_projects')}}
),
data_mapped AS (
    SELECT
        s.tsh_retroresult_id,
        s.note,
        s.point,
        s.tsh_user_id,
        s.user_type,
        s.user_level,
        s.position_id,
        s.branch_id,
        s.project_id,
        r.tsh_retro_id,
        r.status AS retro_status,
        r.start_date,
        r.end_date,
        project_tsh.tsh_project_code
    FROM
        source s
    INNER JOIN base_tsh_retros r ON s.retro_id = r.tsh_retro_id
    INNER JOIN project_tsh ON s.project_id = project_tsh.tsh_project_id
)
SELECT
    *
FROM
    data_mapped
