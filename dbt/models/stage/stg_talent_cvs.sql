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
        is_deleted IS FALSE
),
base_cvsource AS(
    SELECT
        *
    FROM
        {{ ref('base_tal_cvsources') }}
    WHERE
        is_deleted IS FALSE
),
base_cvskill AS(
    SELECT
        *
    FROM
        {{ ref('base_tal_cvskills') }}
    WHERE
        is_deleted = FALSE
),
data_mapping AS(
    SELECT
        bcv.tal_cv_candidate_id AS tal_cv_candidate_id,
        bcv.name AS NAME,
        bcv.note,
        bcv.email,
        bcv.phone,
        bcv.address,
        bcv.birthday,
        bcv.branch as branch_id,
        bcv.cvstatus AS cv_status,
        bcv.is_female,
        bcv.tenant_id,
        bcv.user_type,
        bcv.reference_id,
        bcv.creation_time AS creation_time,
        bcv.deletion_time,
        bcv.sub_position_id,
        bcv.last_modification_time,
        bcv.creator_user_id,
        bcv.last_modifier_user_id,
        bs.tal_cv_source_id,
        bs.name AS source_name
    FROM
        base_cv bcv
    LEFT JOIN base_cvsource bs
        ON bcv.cv_source_id = bs.tal_cv_source_id
)
SELECT
    *
FROM
    data_mapping
