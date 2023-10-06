{{ config(
    materialized = 'table',
    schema = 'stage'
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ ref('base_tsh_projects') }}
),
data_type_rename_conversion AS(
    SELECT
        tsh_project_id,
        tsh_project_code,
        project_name,
        project_note,
        tsh_project_status,
        end_end,
        start_time,
        tsh_customer_id,
        tsh_project_type,
        is_general,
        is_deleted,
        creation_time,
        deletion_time,
        last_modification_time
    FROM
        source
)
SELECT
    *
FROM
    data_type_rename_conversion
