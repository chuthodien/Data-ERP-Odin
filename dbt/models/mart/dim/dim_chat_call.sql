{{ config(
    materialized = 'table',
    schema = 'mart'
) }}

WITH stg_call_joins AS (
    SELECT
        *
    FROM
        {{ ref('stg_call_joins') }}

),
dim_employee AS (
    SELECT
        *
    FROM 
        {{ ref('dim_employee')}}
),
data_mapped AS (
    SELECT
        scj.*,
        {{time_diff_ms('start_time_normalized', 'end_time_normalized')}}::INTEGER AS "duration",
        de.*
    FROM
        stg_call_joins scj
    LEFT JOIN dim_employee de
        ON scj.employee_email = de.email
)
SELECT
    *
FROM
    data_mapped
