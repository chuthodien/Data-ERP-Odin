{{ config(
    materialized = 'table',
    schema = 'stage'
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ ref(
            'base_hrm_salaryexecutions'
        ) }}
),
data_type_rename_conversion AS (
    SELECT
        "hrm_salaryexecutions_id",
        "year",
        "month",
        "salary",
        "hrm_user_id",
        "end_date",
        "start_date",
        "is_deleted",
        "creation_time",
        "deletion_time",
        "last_modification_time"
    FROM
        source
)
SELECT
    *
FROM
    data_type_rename_conversion
