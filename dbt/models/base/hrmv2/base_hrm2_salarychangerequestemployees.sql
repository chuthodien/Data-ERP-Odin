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
            'hrm_salarychangerequestemployees'
        ) }}
),
data_type_rename_conversion AS (
    SELECT
        "Id" :: INT AS "hrm_salarychangerequestemployees_id",
        "note" :: VARCHAR AS "note",
        "Type" :: INT AS "type",
        "salary" :: INT AS "salary",
        "levelid" :: INT AS "level_id",
        "tenantid" :: INT AS "tenant_id",
	"tosalary" :: INT AS "to_salary",
	 {{ to_timestamp("applydate") }} :: TIMESTAMP AS "apply_date",
        "isdeleted" :: BOOLEAN AS "is_deleted",
	"tolevelid" :: INT AS "to_level_id",	
	"employeeid" :: INT AS "employee_id",	
	"tousertype" :: INT AS "touser_type",
	"hascontract" :: BOOLEAN AS "has_contract",	
        {{ to_timestamp("creationtime") }} :: TIMESTAMP AS "creation_time",
        {{ to_timestamp("deletiontime") }} :: TIMESTAMP AS "deletion_time",
        "fromusertype" :: INT AS "from_user_type",
        "creatoruserid" :: INT AS "creatoruser_id",
        "deleteruserid" :: INT AS "deleteuser_id",
        "jobpositionid" :: INT AS "jobposition_id",
        "tojobpositionid" :: INT AS "to_jobposition_id",
        "lastmodifieruserid" :: INT AS "lastmodifieruser_id",
        {{ to_timestamp("lastmodificationtime") }} :: TIMESTAMP AS "last_modification_time",
	"salarychangerequestid" :: INT AS "salary_change_request_id"
    FROM
        source
)
SELECT
    *
FROM
    data_type_rename_conversion
