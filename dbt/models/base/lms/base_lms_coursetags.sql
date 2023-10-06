{{ config(
    materialized = 'view',
    schema = 'base'
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ source(
            'raw_lms',
            'lms_coursetags'
        ) }}
),

data_type_rename_conversion AS (
    SELECT
        "id" :: VARCHAR AS "coursetags_id",
        "courseid" :: VARCHAR AS "course_id",
        "categoryid" :: VARCHAR AS "category_id"
    FROM
        source
)

SELECT
    *
FROM
    data_type_rename_conversion
