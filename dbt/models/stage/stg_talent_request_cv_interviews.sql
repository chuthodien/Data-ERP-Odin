{{ config(
    materialized = 'table',
    schema = 'stage'
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ ref('base_tal_requestcvinterviews') }}
    WHERE
        is_deleted IS FALSE
),
data_selection AS(
    SELECT
        tal_request_cv_interview_id AS request_cv_interview_id,
        interview_id,
        request_cv_id,
        creation_time,
        deletion_time,
        creator_user_id,
        deleter_user_id,
        last_modification_time,
        last_modifier_user_id
    FROM
        source
)
SELECT
    *
FROM
    data_selection
