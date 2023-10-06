{{ config(
    materialized = 'view',
    schema = 'base'
) }}

WITH source AS(

    SELECT
        *
    FROM
        {{ source(
            "raw_talent",
            "tal_cvs"
        ) }}
),
data_type_rename_conversion AS(
    SELECT
        "Id" :: INT AS "tal_cv_candidate_id",
        "Name" :: VARCHAR AS "name",
        note,
        email,
        phone,
        avatar,
        linkcv AS link_cv,
        address,
        {{ to_timestamp("birthday") }} :: TIMESTAMP AS birthday,
        branchid :: INT AS branch,
        cvstatus :: INT,
        isfemale :: BOOLEAN AS is_female,
        nccemail AS ncc_email,
        tenantid :: INT AS tenant_id,
        usertype :: INT AS user_type,
        isdeleted :: BOOLEAN AS is_deleted,
        cvsourceid AS cv_source_id,
        referenceid :: INT AS reference_id,
        {{ to_timestamp("creationtime") }} :: TIMESTAMP AS creation_time,
        {{ to_timestamp("deletiontime") }} :: TIMESTAMP AS deletion_time,
        creatoruserid :: INT AS creator_user_id,
        deleteruserid :: INT AS deleter_user_id,
        subpositionid :: INT AS sub_position_id,
        lastmodifieruserid :: INT AS last_modifier_user_id,
        lastmodificationtime :: TIMESTAMP AS last_modification_time
    FROM
        source
)
SELECT
    *
FROM
    data_type_rename_conversion
