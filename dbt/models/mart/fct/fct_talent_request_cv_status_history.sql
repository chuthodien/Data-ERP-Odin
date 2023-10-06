{{ config(
    materialized = 'table',
    schema = 'mart'
) }}

WITH status_history AS(

    SELECT
        *,
        time_at - LEAD(
            time_at,
            1
        ) over(
            PARTITION BY request_cv_id
            ORDER BY
                status DESC,
                time_at DESC
        ) AS process_time
    FROM
        {{ ref('dim_talent_request_cv_status_history') }}
),
talent_user AS (
    SELECT
        *
    FROM
        {{ ref('dim_talent_users') }}
),
data_mapped AS (
    SELECT
        sh.*,
        EXTRACT(
            epoch
            FROM
                process_time
        ) / 3600 AS process_hour,
        tu.username
    FROM
        status_history sh
        LEFT JOIN talent_user tu
        ON sh.creator_user_id = tu.talent_user_id
)
SELECT
    *
FROM
    data_mapped
