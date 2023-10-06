{{ config(
    materialized = 'table',
    schema = 'stage'
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ ref(
            'base_hrm_workingcontracts'
        ) }}
),
data_type_rename_conversion AS (
    SELECT
        "hrm_workingcontracts_id",
        "hrm_user_id",
        "contract_name",
        "contract_salary",
        "end_date",
        "start_date",
        "is_deleted",
        "creation_time",
        "deletion_time",
        "last_modification_time",
        {{add_date_at('start_date')}}
    FROM
        source
)
SELECT
    *
FROM
    data_type_rename_conversion
