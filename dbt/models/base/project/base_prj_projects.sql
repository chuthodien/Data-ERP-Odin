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
            'prj_projects'
        ) }}
),
data_type_rename_conversion AS (
    SELECT
        "Id" :: INT AS "prj_project_id",
        "code" :: VARCHAR AS "project_code",
        "Name" :: VARCHAR AS "project_name",
        "pmid" :: INT AS "pm_user_id",
        "status" :: INT AS "project_status",
        (CASE 
            WHEN "endtime" IS NOT NULL THEN endtime
            ELSE NULL
        END) :: TIMESTAMP AS "end_time",
        "clientid" :: INT AS "client_id",
        "ischarge" :: BOOLEAN AS "is_charge",
        "isdeleted" :: BOOLEAN AS "is_deleted",
        {{ to_timestamp("starttime") }} :: TIMESTAMP AS "start_time",
        "chargetype" :: VARCHAR AS "charge_type",
        "currencyid" :: INT AS "currency_id",
        "evaluation" :: VARCHAR AS "evaluation",
        "projecttype" :: INT AS "project_type",
        {{ to_timestamp("creationtime") }} :: TIMESTAMP AS "creation_time",
        {{ to_timestamp("deletiontime") }} :: TIMESTAMP AS "deletion_time",
        "newknowledge" :: text AS "new_knowledge",
        "otherproblems" :: text AS "other_problems",
        "technologyused" :: text AS "technology_used",
        "briefdescription" :: text AS "brief_description",
        "detaildescription" :: text AS "detail_description",
        "technicalproblems" :: text AS "technical_problems",
        {{ to_timestamp("lastmodificationtime") }} :: TIMESTAMP AS "last_modification_time"
    FROM
        source
)
SELECT
    *
FROM
    data_type_rename_conversion
