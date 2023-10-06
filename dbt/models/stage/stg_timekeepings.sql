{{ config(
    materialized = 'table',
    schema = 'stage'
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ ref(
            'base_tsh_timekeepings'
        ) }}
    WHERE is_deleted IS FALSE
), 
stg_employee AS (
    SELECT
        *
    FROM
     {{ ref('stg_employee' )}} e
),
data_type_rename_conversion AS (
    SELECT
        s.*
        ,se.email AS employee_email
        ,{{add_date_at('time_at')}}
    FROM
        source s
    LEFT JOIN stg_employee se
        ON s.tsh_user_id = se.tsh_user_id
    WHERE se.email is not null
)
SELECT
    *
FROM
    data_type_rename_conversion
