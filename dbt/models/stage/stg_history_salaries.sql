{{ config(
    materialized = 'table',
    schema = 'base'
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ ref(
            'base_hrm_historysalaries'
        ) }}
),
data_type_rename_conversion AS (
    SELECT
        "hrm_historysalaries_id",
        "amount",
        "hrm_user_id",
        "hrm_request_id",
        "end_date",
        "start_date",
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
