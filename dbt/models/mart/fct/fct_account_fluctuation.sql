{{ config(
    schema = 'mart',
    materialized = "table"
) }}

WITH dim_account_fluctuation AS (
    SELECT
        *
    FROM
        {{ ref('dim_account_fluctuation') }}
),
stg_projects AS (
    SELECT
        *
    FROM
        {{ ref('stg_projects') }}
),
stg_clients AS (
    SELECT
        *
    FROM
        {{ ref('stg_clients') }}
),
accounts_start AS (
    SELECT
        prj_user_id AS user_id,
        stg_clients.client_code,
        dim_account_fluctuation.project_id,
        dim_account_fluctuation.start_month_date AS date_at,
        5 :: INT AS fluctuation,
        1 :: INT AS account_value,
        1 :: INT AS increase_value,
        0 :: INT AS decrease_value
    FROM
        dim_account_fluctuation
    JOIN stg_projects 
	    ON stg_projects.prj_project_id = dim_account_fluctuation.project_id
    JOIN stg_clients
        ON stg_projects.client_id = stg_clients.prj_clients_id
    WHERE is_new = TRUE
),
accounts_end AS (
    SELECT
        prj_user_id AS user_id,
        stg_clients.client_code,
        dim_account_fluctuation.project_id,
        dim_account_fluctuation.start_month_date AS date_at,
        6 :: INT AS fluctuation,
        -1 :: INT AS account_value,
        0 :: INT AS increase_value,
        -1 :: INT AS decrease_value
    FROM
        dim_account_fluctuation
    JOIN stg_projects 
	    ON stg_projects.prj_project_id = dim_account_fluctuation.project_id
    JOIN stg_clients
        ON stg_projects.client_id = stg_clients.prj_clients_id
    WHERE is_ended = TRUE
),
data_maped AS (
    SELECT
        *
    FROM
        accounts_start
    UNION
    SELECT
        *
    FROM
        accounts_end
)
SELECT
    *
FROM
    data_maped
ORDER BY
    data_maped.date_at DESC
