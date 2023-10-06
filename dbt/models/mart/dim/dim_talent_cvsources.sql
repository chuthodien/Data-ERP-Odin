{{ config(
    materialized = 'table',
    schema = 'mart'
) }}

WITH source AS(

    SELECT
        *
    FROM
        {{ ref('stg_talent_cvs') }}
),
data_selection AS(
    SELECT
        tal_cv_candidate_id,
        NAME AS cv_name,
        branch,
        cv_status,
        user_type,
        reference_id,
        creation_time,
        deletion_time,
        last_modification_time,
        tal_cv_source_id AS source_id,
        (
            CASE
                WHEN source_name IS NOT NULL THEN source_name
                ELSE 'unknown'
            END
        ) :: VARCHAR AS "source_name"
    FROM
        source
)
SELECT
    *
FROM
    data_selection
