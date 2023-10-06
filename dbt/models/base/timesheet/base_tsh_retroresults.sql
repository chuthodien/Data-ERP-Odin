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
            'tsh_retroresults'
        ) }}
    WHERE isdeleted IS FALSE
),
data_type_rename_conversion AS (
    SELECT
        "Id" :: INT AS "tsh_retroresult_id",
        "note" :: VARCHAR AS "note",
        "point",
        "userid" :: INT AS "tsh_user_id",
        "retroid" :: INT AS "retro_id",
        "branchid" :: INT AS "branch_id",
        "usertype" :: INT AS "user_type",
        "projectid" :: INT AS "project_id",
        "userlevel" :: INT AS "user_level",
        "positionid" :: INT AS "position_id",
	    "creatoruserid" :: INT AS "creatoruser_id",
        {{ extract_meta_columns() }}
    FROM
        source
)
SELECT
    *
FROM
    data_type_rename_conversion
