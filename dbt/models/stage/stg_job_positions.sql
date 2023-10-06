{{ config(
    materialized = 'table',
    schema = 'stage'
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ ref('base_hrm2_jobpositions') }}
    WHERE
        is_deleted IS FALSE
),
data_type_rename_conversion AS (
    SELECT
        hrm_jobpositions_id AS job_position_id,
        code AS job_position_code,
        NAME AS job_position_name,
        color AS job_position_color,
        shortname AS job_position_shortname
    FROM
        source
)
SELECT
    *
FROM
    data_type_rename_conversion
