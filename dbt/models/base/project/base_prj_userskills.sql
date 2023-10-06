{{ config(
    materialized = 'view',
    schema = 'base'
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ source(
            'raw_project',
            'prj_userskills'
        ) }}
),
data_type_rename_conversion AS (
    SELECT
        "Id" :: INT AS "prj_userskill_id",
        "userid" :: INT AS "prj_user_id",
        "skillid" :: INT AS "prj_skill_id",
        {{ extract_meta_columns() }}
    FROM
        source
)
SELECT
    *
FROM
    data_type_rename_conversion
