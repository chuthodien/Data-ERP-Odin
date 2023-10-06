{{ config(
    materialized = 'view',
    schema = 'base'
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ source(
            'raw_talent',
            'tal_employeeworkingexperiences'
        ) }}
),
data_type_rename_conversion AS(
    SELECT
        "Id" :: INT AS "tal_employee_working_experience_id",
        "Order" :: INT AS "order",
        userid :: INT AS user_id,
        {{ to_timestamp("starttime") }} :: TIMESTAMP AS start_time,
        {{ to_timestamp("endtime") }} :: TIMESTAMP AS end_time,
        "Position" :: VARCHAR AS "position",
        tenantid :: INT AS tenant_id,
        isdeleted :: BOOLEAN AS is_deleted,
        projectid :: INT AS project_id,
        versionid :: INT AS version_id,
        projectname :: VARCHAR AS project_name,
        projectdescription :: VARCHAR project_description,
        {{ to_timestamp("creationtime") }} :: TIMESTAMP AS creation_time,
        {{ to_timestamp("deletiontime") }} :: TIMESTAMP AS deletion_time,
        technologies :: VARCHAR AS technologies,
        creatoruserid :: INT AS creator_user_id,
        deleteruserid :: INT AS deleter_user_id,
        responsibilities :: VARCHAR AS responsibilities,
        lastmodifieruserid :: INT AS last_modifier_user_id,
        {{ to_timestamp("lastmodificationtime") }} :: TIMESTAMP AS last_modification_time
    FROM
        source
)
SELECT
    *
FROM
    data_type_rename_conversion
