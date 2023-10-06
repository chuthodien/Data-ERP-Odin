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
            'prj_projecttechnologies'
        ) }}
),
data_type_rename_conversion AS (
    SELECT
        "Id" :: VARCHAR AS "prj_projecttechnology_id",
        "projectid" :: INT AS "prj_project_id",
        "technologyid" :: INT AS "prj_technology_id",
        {{ extract_meta_columns() }}
    FROM
        source
)
SELECT
    *
FROM
    data_type_rename_conversion
