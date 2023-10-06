{{
	config (
		materialized='view',
		schema='base'
	)
}}
WITH source AS (
	SELECT 
		* 
	FROM {{source(
		'raw_hrmv2',
		'hrm_employeecontracts'
	)}}
),
data_type_rename_conversion AS (
    SELECT
        "Id" :: INT AS "hrm_employeecontracts_id",
        "code" :: VARCHAR AS "code",
        "File" :: VARCHAR AS "file",
        "note" :: VARCHAR AS "note",
        "levelid" :: INT AS "level_id",
        "usertype" :: INT AS "user_type",
	"tenantid" :: INT AS "tenant_id",
	"isdeleted" :: BOOLEAN AS "is_deleted",
        {{ to_timestamp("startdate") }} :: TIMESTAMP AS "start_date",
	"employeeid" :: INT AS "employee_id",
	"realsalary" :: FLOAT AS "real_salary",
	"basicsalary" :: INT AS "basic_salary",
        {{ to_timestamp("creationtime") }} :: TIMESTAMP AS "creation_time",
        {{ to_timestamp("deletiontime") }} :: TIMESTAMP AS "deletion_time",
        "creatoruserid" :: INT AS "creatoruser_id",
        "deleteruserid" :: INT AS "deleteruser_id",
	"jobpositionid" :: INT AS "job_position_id",
        "lastmodifieruserid" :: INT AS "lastmodifieruser_id",
	"probationpercentage" :: INT AS "probation_percentage",
	{{ to_timestamp("lastmodificationtime") }} :: TIMESTAMP AS "last_modification_time",
	"salaryrequestemployeeid" :: INT AS "salaryrequest_employeeid"
    FROM
        source
)
SELECT
    *
FROM
    data_type_rename_conversion
