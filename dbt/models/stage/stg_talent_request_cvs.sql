{{ config(
    materialized = 'table',
    schema = 'stage'
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ ref('base_tal_requestcvs') }}
    WHERE
        is_deleted IS FALSE
),
data_type_rename_conversion AS(
    SELECT
        tal_request_cv_id AS request_cv_id,
        cv_id,
        hrnote,
        salary,
        status,
        lmsinfo,
        tenant_id,
        request_id,
        apply_level,
        final_level,
        interview_time,
        interview_level,
        creation_time,
        deletion_time,
        last_modification_time,
        onboard_date,
        creator_user_id, 
        last_modifier_user_id
    FROM
        source
)
SELECT
    *
FROM
    data_type_rename_conversion
