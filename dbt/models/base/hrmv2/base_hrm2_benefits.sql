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
            'hrm_benefits'
        ) }}
),
data_type_rename_conversion AS (
    SELECT
        "Id" :: INT AS "hrm_benefits_id",
        "Name" :: VARCHAR AS "name",
        "Type" :: INT AS "type",
        "money" :: INT AS "money",
        "isactive" :: BOOLEAN AS "is_active",
        "tenantid" :: INT AS "tenant_id",
        {{ to_timestamp("applydate") }} :: TIMESTAMP AS "apply_date",
        "isdeleted" :: BOOLEAN AS "is_deleted",
        {{ to_timestamp("creationtime") }} :: TIMESTAMP AS "creation_time",
        {{ to_timestamp("deletiontime") }} :: TIMESTAMP AS "deletion_time",
        "creatoruserid" :: INT AS "creatoruser_id",
        "deleteruserid" :: INT AS "deleteruser_id",
        "lastmodifieruserid" :: INT AS "lastmodifieruser_id",
        {{ to_timestamp("lastmodificationtime") }} :: TIMESTAMP AS "last_modification_time",
        "isbelongtoallemployee" :: BOOLEAN AS "is_belong_toallemployee"
    FROM
        source
)
SELECT
    *
FROM
    data_type_rename_conversion
