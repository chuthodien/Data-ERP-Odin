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
            'tsh_retros'
        ) }}
    WHERE isdeleted IS FALSE
),
data_type_rename_conversion AS (
    SELECT
        "Id" :: INT AS "tsh_retro_id",
        "Name" :: VARCHAR AS "name",
        "status",
        "startdate" :: TIMESTAMP AS "start_date",
        "enddate" :: TIMESTAMP AS "end_date",
        "deadline" :: TIMESTAMP AS "deadline",
	    "creatoruserid" :: INT AS "creatoruser_id",
        {{ extract_meta_columns() }}
    FROM
        source
)
SELECT
    *
FROM
    data_type_rename_conversion
