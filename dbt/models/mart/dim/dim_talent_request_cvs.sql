{{ config (
    materialized = 'table',
    schema = 'mart'
) }}

WITH request_cv AS (
    SELECT
        *,
        {{dbt_utils.date_trunc('month', "creation_time")}} AS create_at_month,
        {{dbt_utils.date_trunc('month', "last_modification_time")}} AS last_update_at_month
    FROM
        {{ ref('stg_talent_request_cvs') }}
),
request_cv_status AS(
    SELECT
        request_cv_status_id AS status_id,
        request_cv_status_code :: VARCHAR AS status_code
    FROM
        {{ ref('request_cv_status') }}
),
data_mapped AS(
    SELECT
        request_cv_id,
        cv_id,
        request_id,
        salary,
        rc.status,
        rcs.status_code,
        apply_level,
        final_level,
        interview_time,
        creation_time,
        deletion_time,
        last_modification_time,
        creator_user_id, 
        last_modifier_user_id,
        create_at_month,
        last_update_at_month
    FROM
        request_cv rc
        LEFT JOIN request_cv_status rcs
        ON rc.status = rcs.status_id
)
SELECT
    *
FROM
    data_mapped
