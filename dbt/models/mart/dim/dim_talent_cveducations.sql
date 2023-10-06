{{ config(
    materialized = 'view',
    schema = 'mart'
) }}

WITH source AS(

    SELECT
        tal_cv_education_id,
        tal_cv_candidate_id,
        cv_name,
        education_id,
        (CASE
            WHEN "education_name" IS NOT NULL THEN education_name
            ELSE 'unknown'
        END) :: VARCHAR AS "education_name",
        branch,
        cvstatus,
        user_type,
        cv_creation_time,
        deletion_time,
        cv_last_modification_time,
        cv_edu_creation_time,
        cv_edu_last_modification_time,
        creator_user_id
    FROM
        {{ ref('stg_talent_cveducations') }}
),
data_selection AS(
    SELECT
        *
    FROM
        source
)
SELECT
    *
FROM
    data_selection