{{ config(
    materialized = 'view',
    schema = 'base'
) }}

WITH source AS(

    SELECT
        *
    FROM
        {{ source(
            'raw_tracker',
            'raw_reports'
        ) }}
),
data_type_rename_conversion AS (
    SELECT
        "Id" :: INT AS "tracker_report_id",
        "wfh" :: BOOLEAN AS "wfh",
        {{ to_timestamp("date") }} :: TIMESTAMP AS "date",
        "email" :: VARCHAR AS "email",
        "call_time" :: FLOAT AS "call_time",
        "spent_time" :: FLOAT AS "spent_time"
    FROM
        source
)
SELECT
    *
FROM
    data_type_rename_conversion
