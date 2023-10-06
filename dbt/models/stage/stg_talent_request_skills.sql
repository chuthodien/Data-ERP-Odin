{{ config(
    materialized = 'table',
    schema = 'stage'
) }}

WITH request_skill AS (

    SELECT
        tal_request_skill_id,
        skill_id,
        is_deleted,
        request_id,
        creation_time,
        deletion_time,
        last_modification_time
    FROM
        {{ ref('base_tal_requestskills') }}
    WHERE
        is_deleted IS FALSE
),
skill AS (
    SELECT
        tal_skill_id,
        "name"
    FROM
        {{ ref('base_tal_skills') }}
    WHERE
        is_deleted IS FALSE
),
data_type_rename_conversion AS(
    SELECT
        tal_request_skill_id AS request_skill_id,
        request_id,
        skill_id,
        skill.name,
        creation_time,
        deletion_time,
        last_modification_time
    FROM
        request_skill AS rs
        INNER JOIN skill
        ON rs.skill_id = skill.tal_skill_id
)
SELECT
    *
FROM
    data_type_rename_conversion
