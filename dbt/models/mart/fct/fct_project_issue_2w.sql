{{ config(
    schema = 'mart',
    materialized = "table"
) }}

WITH dim_project_issue AS (
    SELECT
        *
    FROM
        {{ ref('dim_project_issue') }}
),
project_issue_status AS (
    SELECT
        *
    FROM
        {{ ref('project_issue_status') }}
),
data_maped AS (
    SELECT 
        dpi.*,
        pis.issue_status_code,
        pis.issue_status_name,
        ct.critical_type_name
    FROM
        dim_project_issue dpi
    JOIN project_issue_status pis
    ON pis.issue_status = dpi.issue_status
    JOIN public.critical_types ct
    ON dpi.critical::INTEGER = ct.critical_type_id
),
data_aggreated AS (
    SELECT
        dm.*
    FROM
        data_maped dm
    WHERE project_issue_creation_time >= CURRENT_DATE - INTERVAL '4 weeks'
    AND dm.issue_status = 0
)
SELECT
    dg.*
FROM
    data_aggreated dg
