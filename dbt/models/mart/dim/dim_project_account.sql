{{ config(
    materialized = 'table',
    schema = 'mart'
) }}

WITH stg_project_account AS (
    SELECT
        *
    FROM {{ ref('stg_project_account')}}
),
stg_projects AS (
    SELECT
        *
    FROM {{ ref('stg_projects')}}
),
data_mapped AS (
    SELECT
        {{add_date_at('project_account_creation_time')}},
        spa.*,
        sp.*,
        pst.*,
        pt.*,
        dc.*
    FROM stg_project_account spa
    LEFT JOIN stg_projects sp
        ON sp.prj_project_id = spa.project_id
    LEFT JOIN {{ref('project_status')}} pst
        ON sp.project_status = pst.project_status_id
    LEFT JOIN {{ref('project_types')}} pt
        ON sp.project_type = pt.project_type_id
    LEFT JOIN {{ref('currencies')}} c
        ON sp.currency_id = c.currency_id
    LEFT JOIN {{ref('dim_client')}} dc
        ON dc.prj_clients_id = sp.client_id
)
SELECT
    *
FROM
    data_mapped
