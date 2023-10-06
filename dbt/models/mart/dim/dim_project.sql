{{ config(
    materialized = 'table',
    schema = 'mart'
) }}

WITH stg_projects AS (

    SELECT
        *
    FROM
        {{ ref(
            'stg_projects'
        ) }}
)
SELECT
    {{add_date_at('start_time')}},
    sp.*,
    pst.*,
    pt.*,
    dc.*
FROM
    stg_projects sp
LEFT JOIN {{ref('project_status')}} pst
    ON sp.project_status = pst.project_status_id
LEFT JOIN {{ref('project_types')}} pt
    ON sp.project_type = pt.project_type_id
LEFT JOIN {{ref('currencies')}} c
    ON sp.currency_id = c.currency_id
LEFT JOIN {{ref('dim_client')}} dc
    ON dc.prj_clients_id = sp.client_id
