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
cv_email_status AS (
    SELECT
        *
    FROM
        {{ ref('dim_talent_email_status') }}
),
requests AS (
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
data_mapped AS (
    SELECT
        rc.request_cv_id AS request_cv_id,
        rc.cv_id,
        cvs.email,
        rc.request_id,
        r.sub_position_name AS requested_sub_position,
        rc.status AS interview_status,
        rc.status_code AS interview_status_code,
        ces.id AS email_status_id,
        ces.email_status,
        rc.salary,
        rc.apply_level,
        rc.final_level,
        ces.description AS email_description,
        rc.last_modification_time AS interview_status_last_modification_time,
        rc.interview_time,
        ces.creation_time AS email_creation_time
    FROM
        request_cvs rc
        LEFT JOIN cv_email_status ces
        ON rc.cv_id = ces.cv_id
        LEFT JOIN requests r
        ON rc.request_id = r.request_id
        LEFT JOIN cvs
        ON cvs.talent_cv_id = rc.cv_id
)
SELECT
    *
FROM
    data_mapped
