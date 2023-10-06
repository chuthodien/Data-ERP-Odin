{{ config(
    materialized = 'view',
    schema = 'base'
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ source(
            'hrm_sheets',
            'hrm_employee_start_date'
        ) }}
),
data_type_rename_conversion AS (
    SELECT
        "emailaddress" :: VARCHAR AS "hrms_email",
        TO_DATE(
            "startdate",
            'YYYY-MM-DD'
        ) :: DATE AS "hrms_contract_start_date"
    FROM
        source
),
data_mapping AS (
    SELECT
        *
    FROM
        data_type_rename_conversion
)
SELECT
    *
FROM
    data_mapping
