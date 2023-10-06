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
            'prj_projectuserbills'
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
        "Id" :: INT AS "prj_projectuserbills_id",
        "note" :: VARCHAR AS "user_bill_note",
        "userid" :: INT AS "prj_user_id",
        "endtime" :: TIMESTAMP AS "end_time",
        "billrate" :: FLOAT AS "bill_rate",
        "billrole" :: VARCHAR AS "bill_role",
        "isactive" :: BOOLEAN AS "is_active",
        "projectid" :: INT AS "prj_project_id",
        "starttime" :: TIMESTAMP AS "start_time",
        "shadownote" :: VARCHAR AS "shadow_note",
        {{ extract_meta_columns() }}
    FROM
        data_selection
)
SELECT
    *
FROM
    data_type_rename_conversion
