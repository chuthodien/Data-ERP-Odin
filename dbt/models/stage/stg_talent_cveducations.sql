{{ config(
    materialized = 'table',
    schema = 'stage'
) }}

WITH base_cv AS(
    SELECT
        *
    FROM
        {{ ref('base_tal_cvs') }}
    WHERE
        is_deleted = false
),
base_cveducation AS(
    SELECT
        *
    FROM
        {{  ref('base_tal_cveducations')}}
    WHERE
        is_deleted = false
),
base_education AS(
    SELECT
        *
    FROM
        {{  ref('base_tal_educations')}}
    WHERE
        is_deleted = false
),
data_mapping AS(
    SELECT
        bcve.tal_cv_education_id,
        bcv.tal_cv_candidate_id,
        bcv.name as cv_name,
        bcve.education_id,
        be.name as education_name,
        bcv.branch,
        bcv.cvstatus,
        bcv.user_type,
        bcv.creation_time as cv_creation_time,
        bcv.last_modification_time as cv_last_modification_time,
        bcv.is_deleted,
        bcv.deletion_time,
        bcve.creation_time as cv_edu_creation_time,
        bcve.last_modification_time as cv_edu_last_modification_time,
        bcve.creator_user_id
    FROM
        base_cv bcv
    LEFT JOIN base_cveducation bcve
    ON bcve.cv_id = bcv.tal_cv_candidate_id
    LEFT JOIN base_education be
    ON bcve.education_id = be.tal_education_id
    ORDER BY bcv.creation_time
)
SELECT
    *
FROM
    data_mapping
