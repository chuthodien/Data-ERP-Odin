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
            'prj_resourcerequestskills'
        ) }}
),
data_selection AS (
    SELECT
        *
    FROM
        source
),
data_type_rename_conversion AS (
    SELECT
        "Id" :: VARCHAR AS "prj_resourcerequestskills_id",
        "skillid" :: INT AS "prj_skill_id",
        "quantity" :: INT AS "quantity",
        "resourcerequestid" :: INT AS "prj_resourcerequest_id",
        {{ extract_meta_columns() }}
    FROM
        data_selection
)
SELECT
    *
FROM
    data_type_rename_conversion
