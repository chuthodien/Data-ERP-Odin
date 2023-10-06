{{ config(
    materialized = 'table',
    schema = 'mart'
) }}

WITH talent_cvs AS(

    SELECT
        *
    FROM
        {{ ref('stg_talent_cvs') }}
),
cv_status AS (
    SELECT
        *
    FROM
        {{ ref('cv_status') }}
),
user_type AS (
    SELECT
        *
    FROM
        {{ ref('talent_user_types') }}
),
sub_position AS(
    SELECT
        *
    FROM
        {{ ref('stg_talent_sub_positions') }}
),
data_mapped AS(
    SELECT
        tal_cv_candidate_id AS talent_cv_id,
        NAME,
        note,
        email,
        phone,
        address,
        birthday,
        talent_cvs.cv_status AS cv_status_id,
        cv_status.cv_status_code,
        is_female,
        talent_cvs.tenant_id,
        user_type,
        user_type.user_type_code,
        reference_id,
        talent_cvs.creation_time,
        talent_cvs.deletion_time,
        talent_cvs.sub_position_id,
        sub_position.sub_position_name,
        talent_cvs.last_modification_time,
        tal_cv_source_id AS source_id,
        talent_cvs.source_name,
        talent_cvs.creator_user_id,
        talent_cvs.last_modifier_user_id,
        {{dbt_utils.date_trunc('month', "talent_cvs.creation_time")}} AS create_at_month,
        {{dbt_utils.date_trunc('month', "talent_cvs.last_modification_time")}} AS last_update_at_month
    FROM
        talent_cvs
        INNER JOIN cv_status
        ON talent_cvs.cv_status = cv_status.cv_status_id
        LEFT JOIN user_type
        ON talent_cvs.user_type = user_type.user_type_id
        LEFT JOIN sub_position
        ON talent_cvs.sub_position_id = sub_position.sub_position_id
)
SELECT
    *
FROM
    data_mapped
