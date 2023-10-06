{{ config(
    materialized = 'table',
    schema = 'stage'
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ ref(
            'base_prj_projectuserbills'
        ) }}

), data_selection AS (
    SELECT
        *
    FROM
        source
),
data_type_rename_conversion AS (
    SELECT
        "prj_projectuserbills_id",
        "user_bill_note",
        "prj_user_id",
        "end_time" AS "account_end_time",
        "bill_rate",
        "bill_role",
        "is_active",
        "prj_project_id",
        "start_time" AS "account_start_time",
        "shadow_note",
        {{add_meta_columns('account_bill_')}}     
    FROM
        data_selection
)
SELECT
    *
FROM
    data_type_rename_conversion
