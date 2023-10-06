{{ config(
    materialized = 'table',
    schema = 'mart'
) }}

WITH project_tasks AS (

    SELECT
        *
    FROM
        {{ ref('stg_tsh_project_tasks') }}
),
external_project_ids AS(
    SELECT
        tsh_project_id AS project_id
    FROM
        {{ ref('dim_bill_projects') }}
),
data_mapped AS (
    SELECT
        *
    FROM
        project_tasks AS pt
        INNER JOIN external_project_ids AS epi
        ON pt.tsh_project_id = epi.project_id
)
SELECT
    *
FROM
    data_mapped
