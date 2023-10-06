{{ config(
    materialized = 'table',
    schema = 'stage'
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ ref(
            'base_hrm_usersalaries'
        ) }}
),
data_type_rename_conversion AS (
    SELECT
        "hrm_punishments_id",
        "hrm_user_id",
        "request_id",
        "start_date",
        "salary_real",
        "description",
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
