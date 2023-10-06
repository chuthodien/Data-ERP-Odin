{{ config(
    materialized = 'table',
    schema = 'mart'
) }}

WITH stg_employee_projects AS (
    SELECT
        *
    FROM {{ ref('stg_employee_projects')}}
),
stg_projects AS (
    SELECT
        *
    FROM {{ ref('stg_projects')}}
),
data_mapped AS (
    SELECT
        {{add_date_at('user_start_date')}},
        sep.*,
        sp.*,
        case when sep.is_pool=true then 1 else 0 end  AS "temp",
        case when sep.is_pool=false then 1 else 0 end AS "official",
        pst.*,
        pt.*,
        dc.*
    FROM stg_employee_projects sep
    LEFT JOIN stg_projects sp
        ON sp.prj_project_id = sep.project_id
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
