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
            'tsh_branchs'
        ) }}
    WHERE
        isdeleted IS NOT FALSE
),
data_type_rename_conversion AS (
    SELECT
        "Id" :: INT AS "tsh_branch_id",
        "code" :: VARCHAR AS "tsh_branch_code",
        "Name" :: VARCHAR AS "tsh_branch_name",
        "color" :: VARCHAR AS "tsh_branch_color",
        "displayname" :: VARCHAR AS "tsh_branch_display",
        {{ extract_meta_columns() }}
    FROM
        source
)
SELECT
    *
FROM
    data_type_rename_conversion
