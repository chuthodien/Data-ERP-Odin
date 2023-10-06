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
            'lms_gradeschemeelements'
        ) }}
),

data_type_rename_conversion AS (
    SELECT
        "id" :: VARCHAR AS "gradeschemeelements_id",
        "name" :: VARCHAR AS "name",
        "lowrange" :: INT AS "low_range",
        "tenantid" :: INT AS "tenant_id",
        "highrange" :: INT AS "high_range",
        "isdeleted" :: BOOLEAN AS "is_deleted",
        {{ to_timestamp("creationtime") }} :: TIMESTAMP AS "creation_time",
        {{ to_timestamp("deletiontime") }} :: TIMESTAMP AS "deletion_time",
        "creatoruserid" :: INT AS "creator_user_id",
        "deleteruserid" :: INT AS "deleter_user_id",
        "gradeschemeid" :: VARCHAR AS "grade_scheme_id",
        "lastmodifieruserid" :: INT AS "last_modifier_user_id",
        "highcompareopertion" :: INT AS "high_compare_opertion",
        "lowcompareoperation" :: INT AS "low_compare_operation",
        {{ to_timestamp("lastmodificationtime") }} :: TIMESTAMP AS "last_modification_time"
    FROM
        source
)

SELECT
    *
FROM
    data_type_rename_conversion
