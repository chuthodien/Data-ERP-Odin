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
            'prj_timesheetprojectbills'
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
        "Id" :: VARCHAR AS "prj_timesheetprojectbills_id",
        "note" :: VARCHAR AS "bill_note",
        "userid" :: INT AS "prj_user_id",
        "endtime" :: TIMESTAMP AS "account_end_time",
        "billrate" :: FLOAT AS "bill_rate",
        "billrole" :: VARCHAR AS "bill_role",
        "isactive" :: INT AS "is_active",
        "projectid" :: INT AS "project_id",
        "chargetype" :: INT AS "charge_type",
        "currencyid" :: INT AS "currency_id",
        "starttime" :: TIMESTAMP AS "bill_start_time",
        "shadownote" :: VARCHAR AS "shadow_note",
        "timesheetid" :: INT AS "prj_timesheet_id",
        "workingtime" :: FLOAT AS "working_time",
        {{ extract_meta_columns() }}
    FROM
        data_selection
)
SELECT
    *
FROM
    data_type_rename_conversion
