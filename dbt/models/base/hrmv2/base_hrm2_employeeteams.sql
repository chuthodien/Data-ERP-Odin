{{ config(
    materialized = 'view',
    schema = 'base'
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ source(
            'raw_hrmv2',
            'hrm_employeeteams'
        ) }}
),
data_type_rename_conversion AS (
    SELECT
        "Id" :: INT AS "hrm_employeeteams_id",
        "teamid" :: INT AS "team_id",
        "isdeleted" :: BOOLEAN AS "is_deleted",
	    "employeeid" :: INT AS "employee_id",
        {{ to_timestamp("creationtime") }} :: TIMESTAMP AS "creation_time",
        {{ to_timestamp("deletiontime") }} :: TIMESTAMP AS "deletion_time",
        {{ to_timestamp("lastmodificationtime") }} :: TIMESTAMP AS "last_modification_time"
    FROM
        source
)
SELECT
    *
FROM
    data_type_rename_conversion
