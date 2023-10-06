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
            'tsh_dayoffsettings'
        ) }}
),
data_type_rename_conversion AS (
    SELECT
        "Id" :: INT AS "tsh_dayoffsettings_id",
        "Name" :: VARCHAR AS "day_off_title",
        "branch" :: INT AS "branch",
        {{ to_timestamp('dayoff') }} :: TIMESTAMP AS "day_off",
        "coefficient" :: FLOAT AS "coefficient",
        {{ extract_meta_columns() }}
    FROM
        source
)
SELECT
    *
FROM
    data_type_rename_conversion
