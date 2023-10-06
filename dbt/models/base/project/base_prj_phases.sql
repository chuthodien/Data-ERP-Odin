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
            'prj_phases'
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
        "Id" :: VARCHAR AS "prj_phases_id",
        "Name" :: VARCHAR AS "pharse_name",
        "Type" :: INT AS "pharse_type",
        "Year" :: INT AS "year",
        "Index" :: INT AS "index",
        "status" :: INT AS "pharse_status",
        "parentid" :: INT AS "parent_phrase_id",
        {{ extract_meta_columns() }}
    FROM
        data_selection
)
SELECT
    *
FROM
    data_type_rename_conversion
