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
            'prj_timesheets'
        ) }}
),
data_selection AS (
    SELECT
        *
    FROM
        source
    WHERE
        isdeleted IS FALSE
),
data_type_rename_conversion AS (
    SELECT
        "Id" :: VARCHAR AS "prj_timesheets_id",
        "Name" :: VARCHAR AS "timesheet_name",
        "Year" :: INT AS "year",
        "Month" :: INT AS "month",
        (CONCAT('01', '-', "Month", '-', "Year")) AS "start_month_date_str",
        "isactive" :: BOOLEAN AS "is_active",
        "createdinvoice" :: INT AS "created_invoice",
        "totalworkingday" :: INT AS "total_working_day",
        {{ extract_meta_columns() }}
    FROM
        data_selection
)
SELECT
    *,
    {{ to_timestamp(
        "start_month_date_str",
        "DD-MM-YYYY"
    ) }} AS "start_month_date"
FROM
    data_type_rename_conversion
