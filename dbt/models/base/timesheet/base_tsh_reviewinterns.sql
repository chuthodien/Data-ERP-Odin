{{ config(
    materialized = 'view',
    schema = 'base'
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ source(
            'raw_timesheet',
            'tsh_reviewinterns'
        ) }}
),
data_type_rename_conversion AS (
    SELECT
        "Id" :: INT AS "tsh_reviewinterns_id",
        "Year" :: INT AS "year",
        "Month" :: INT AS "month",
        "isactive" :: BOOLEAN AS "is_active",
        {{ extract_meta_columns() }}
    FROM
        source
)
SELECT
    *
FROM
    data_type_rename_conversion
