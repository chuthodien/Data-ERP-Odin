{{ config(
    materialized = 'table',
    schema = 'mart'
) }}

WITH status_history AS (

    SELECT
        *
    FROM
        {{ ref('stg_talent_request_cv_status_histories') }}
),
request_cv_status AS(
    SELECT
        *
    FROM
        {{ ref('request_cv_status') }}
),
data_mapped AS (
    SELECT
        sh.request_cv_status_history_id,
        sh.status,
        rcs.request_cv_status_code AS status_code,
        sh.time_at,
        sh.request_cv_id,
        sh.creator_user_id
    FROM
        status_history sh
        LEFT JOIN request_cv_status rcs
        ON sh.status = rcs.request_cv_status_id
)
SELECT
    *
FROM
    data_mapped
