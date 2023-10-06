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
            'prj_technologies'
        ) }}
),
data_type_rename_conversion AS (
    SELECT
        "Id" :: INT AS "prj_technology_id",
        "Name" :: VARCHAR AS "technology_name",
        "color" :: VARCHAR AS "technology_color",
        {{ extract_meta_columns() }}
    FROM
        source
)
SELECT
    *
FROM
    data_type_rename_conversion
