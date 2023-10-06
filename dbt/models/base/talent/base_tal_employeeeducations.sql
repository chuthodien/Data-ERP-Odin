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
            'tal_employeeeducations'
        ) }}
),
data_type_rename_conversion AS(
    SELECT
        "Id" :: INT AS "tal_employee_education_id",
        major :: VARCHAR,
        "Order" :: INT AS "order",
        startyear :: INT AS start_year,
        endyear :: INT AS end_year,
        tenantid :: INT AS tenant_id,
        isdeleted :: BOOLEAN AS is_deleted,
        degreetype :: INT AS degree_type,
        description,
        cvemployeeid :: INT AS cv_employee_id,
        {{ to_timestamp("creationtime") }} :: TIMESTAMP AS creation_time,
        {{ to_timestamp("deletiontime") }} :: TIMESTAMP AS deletion_time,
        creatoruserid :: INT AS creator_user_id,
        deleteruserid :: INT AS deleter_user_id,
        lastmodifieruserid :: INT AS last_modifier_user_id,
        schoolorcentername :: VARCHAR AS school_or_center_name,
        {{ to_timestamp("lastmodificationtime") }} :: TIMESTAMP AS last_modification_time
    FROM
        source
)
SELECT
    *
FROM
    data_type_rename_conversion
