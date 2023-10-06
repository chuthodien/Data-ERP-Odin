{{ config(
    materialized = 'table',
    schema = 'stage'
) }}

WITH base_tsh_absencedayrequests AS (

    SELECT
        *
    FROM
        {{ ref('base_tsh_absencedayrequests') }}
),
base_tsh_absencedaydetails AS (
    SELECT
        *
    FROM
        {{ ref('base_tsh_absencedaydetails') }}
),
stg_employee AS (
    SELECT
        *
    FROM
        {{ ref(
            'stg_employee'
        ) }}
)
SELECT
    btar.*,
    {{ add_date_at('btad.date_at') }},
    "hour" :: INT AS "absence_time",
    se.email AS employee_email
FROM
    base_tsh_absencedayrequests btar
    LEFT JOIN base_tsh_absencedaydetails btad
    ON btad.tsh_request_id = btar.tsh_absencedayrequests_id
    LEFT JOIN stg_employee se
    ON se.tsh_user_id = btar.tsh_user_id
WHERE
    se.email IS NOT NULL
