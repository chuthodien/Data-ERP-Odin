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
            "tal_requestcvs"
        ) }}
),
data_type_rename_conversion AS(
    SELECT
        "Id" :: INT AS "tal_request_cv_id",
        cvid :: INT AS cv_id,
        hrnote,
        salary :: INT,
        status :: INT,
        lmsinfo,
        tenantid :: INT AS tenant_id,
        isdeleted :: BOOLEAN AS is_deleted,
        requestid :: INT AS request_id,
        applylevel :: INT AS apply_level,
        finallevel :: INT AS final_level,
        {{ to_timestamp("interviewtime") }} :: TIMESTAMP AS interview_time,
        interviewlevel :: INT AS interview_level,
        {{ to_timestamp("onboarddate") }} :: TIMESTAMP AS onboard_date,
        {{ to_timestamp("creationtime") }} :: TIMESTAMP AS creation_time,
        {{ to_timestamp("deletiontime") }} :: TIMESTAMP AS deletion_time,
        creatoruserid :: INT AS creator_user_id,
        deleteruserid :: INT AS deleter_user_id,
        lastmodifieruserid :: INT AS last_modifier_user_id,
        lastmodificationtime :: TIMESTAMP AS last_modification_time
    FROM
        source
)
SELECT
    *
FROM
    data_type_rename_conversion
