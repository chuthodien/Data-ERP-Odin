{{ config(
    materialized = 'table',
    schema = 'stage'
) }}

WITH source AS (
    SELECT
        *
    FROM
        {{ ref( 'base_komu_joincalls' ) }}

),
stg_employee AS (
    SELECT
        *
    FROM 
        {{ ref('stg_employee')}}
),
data_selection AS (
    SELECT
        *
    FROM
        source
),
data_type_rename_conversion AS (
    SELECT
        call_status,
        ds.komu_user_id AS user_id_komu,
        end_time,
        channel_id,
        start_time,
        start_time_normalized,
        end_time_normalized,
        se.email AS employee_email,
        {{add_date_at('start_time')}} 
    FROM
        data_selection ds
    LEFT JOIN stg_employee se
        ON se.komu_user_id = ds.komu_user_id
)
SELECT
    *
FROM
    data_type_rename_conversion
