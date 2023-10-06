{{ config(
    materialized = 'table',
    schema = 'stage'
) }}

WITH base_tsh_reviewdetails AS (

    SELECT
        *
    FROM {{ref('base_tsh_reviewdetails')}}


), 
stg_employee AS (
    SELECT
        *
    FROM
     {{ ref('stg_employee' )}}
)
SELECT
    {{add_date_at('btr.creation_time')}},
    btr.*,
    se.email AS internship_email,
    se2.email AS reviewer_email
FROM
    base_tsh_reviewdetails btr
LEFT JOIN stg_employee se
    ON se.tsh_user_id =  btr.internship_id
LEFT JOIN stg_employee se2
    ON se2.tsh_user_id = btr.reviewer_id
WHERE se.email is not null
    AND se2.email is not null