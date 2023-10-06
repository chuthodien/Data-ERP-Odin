{{ config(
    schema = 'mart',
    materialized = "table"
) }}

WITH dim_project_pmreport AS (
    SELECT
        *
    FROM
        {{ ref('dim_project_pmreport') }}
),
project_health_status AS (
    SELECT
        *
    FROM
        {{ ref('project_health_status') }}
),
data_maped AS (
    SELECT 
        dpr.*,
        phs.*
    FROM
        dim_project_pmreport dpr
    left join project_health_status phs on dpr.project_health = phs.health_id
),
data_aggreated AS (
    SELECT
        dm.*
    FROM
        data_maped dm
)
SELECT
    dg.*
FROM
    data_aggreated dg