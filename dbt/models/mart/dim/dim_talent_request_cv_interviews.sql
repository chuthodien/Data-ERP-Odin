{{ config(
    materialized = 'table',
    schema = 'mart'
) }}

WITH interview AS (

    SELECT
        *
    FROM
        {{ ref('stg_talent_request_cv_interviews') }}
),
tal_user AS (
    SELECT
        *
    FROM
        {{ ref(
            'stg_talent_users'
        ) }}
),
data_mapped AS (
    SELECT
        i.request_cv_interview_id,
        i.request_cv_id,
        i.creation_time,
        i.creator_user_id,
        i.last_modification_time,
        i.last_modifier_user_id,
        u.username AS interviewer_username,
        u.is_active AS interviewer_is_active
    FROM
        interview i
        LEFT JOIN tal_user u
        ON i.interview_id = u.talent_user_id
)
SELECT
    *
FROM
    data_mapped
