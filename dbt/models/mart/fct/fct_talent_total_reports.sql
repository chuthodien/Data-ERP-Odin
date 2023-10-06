{{ config(
    materialized = 'table',
    schema = 'mart'
) }}

WITH request_cvs AS (

    SELECT
        *
    FROM
        {{ ref('dim_talent_request_cvs') }}
),
request AS (
    SELECT
        *
    FROM
        {{ ref('dim_talent_requests') }}
),
cvs AS (
    SELECT
        *
    FROM
        {{ ref('dim_talent_cvs') }}
),
interview AS(
    SELECT
        *
    FROM
        {{ ref('dim_talent_request_cv_interviews') }}
),
data_mapped AS(
    SELECT
        rc.request_cv_id,
        rc.request_id,
        rc.cv_id,
        rc.status_code AS interview_status,
        rc.apply_level,
        rc.final_level,
        rc.interview_time,
        EXTRACT (
            YEAR
            FROM
                rc.interview_time
        ) AS interview_year,
        EXTRACT (
            MONTH
            FROM
                rc.interview_time
        ) AS interview_month,
        rc.creation_time,
        rc.last_modification_time,
        request.sub_position_name,
        request.request_level,
        request.status AS request_status,
        request.status_code AS request_status_code,
        request.user_type,
        cvs.cv_status_code AS cv_status,
        cvs.source_name,
        interview.interviewer_username,
        interview.interviewer_is_active
    FROM
        request_cvs AS rc
        INNER JOIN request
        ON rc.request_id = request.request_id
        INNER JOIN cvs
        ON rc.cv_id = cvs.talent_cv_id
        LEFT JOIN interview
        ON rc.request_cv_id = interview.request_cv_id
)
SELECT
    *
FROM
    data_mapped
