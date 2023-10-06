{{ config(
    materialized = 'table',
    schema = 'stage'
) }}
WITH contest_result AS (
    SELECT 
        *,
        MAX(contest_point :: INT) OVER () AS max_contest_point
    FROM 
        {{ref('base_contest_result')}}
),
checkpoint_result AS (
    SELECT 
        *,
        MAX(checkpoint_point :: INT) OVER () AS max_checkpoint_point
    FROM 
        {{ref('base_checkpoint_result')}}
),
ranks AS (
    SELECT  
        *
    FROM 
        {{ref('ranks')}}
),
data_mapped AS (
    SELECT 
        COALESCE(cr.employee_email, cpr.employee_email) AS employee_email,
        COALESCE(cr.contest_point, 0) AS contest_point,
        COALESCE({{calc_rank("max_contest_point" , "contest_point", "contest_ranks" )}}, 0) AS contest_rank,
        COALESCE(cpr.checkpoint_point, 0) AS checkpoint_point,
        COALESCE({{calc_rank("max_checkpoint_point" , "checkpoint_point", "checkpoint_ranks" )}}, 0) AS checkpoint_rank
    FROM contest_result cr
    FULL OUTER JOIN checkpoint_result cpr ON cr.employee_email = cpr.employee_email
    LEFT JOIN ranks ON (cr.employee_email IS NOT NULL OR cpr.employee_email IS NOT NULL)
)
SELECT 
    *
FROM 
    data_mapped
