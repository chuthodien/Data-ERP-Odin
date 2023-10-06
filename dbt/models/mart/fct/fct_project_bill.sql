{{ config(
    schema = 'mart',
    materialized = "table"
) }}

WITH dim_project_bill AS (
    SELECT
        *
    FROM
        {{ ref('dim_project_bill') }}
),
data_maped AS (
    SELECT
        dpb.*
    FROM
        dim_project_bill dpb
),
dim_project AS (
    SELECT
        *
    FROM
        {{ ref('dim_project') }}
)
SELECT
    dm.*,
    dp.client_code
FROM
    data_maped dm
    JOIN dim_project dp
    ON dm.project_id = dp.prj_project_id
