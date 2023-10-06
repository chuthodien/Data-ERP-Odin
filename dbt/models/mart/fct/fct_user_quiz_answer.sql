{{ config(
    materialized = 'table',
    schema = 'mart'
) }}
WITH source AS(
    SELECT 
        * 
    FROM 
        {{ref('dim_user_quiz_answer')}}
    WHERE hrm_user_id IS NOT NULL
),
employee_type AS (
    SELECT
        *
    FROM 
        {{ref('employee_types')}}
),
answer_quiz AS (
    SELECT 
        komu_user_id,
        hrm_user_id,
        email, 
        fullname,
        employee_level,
        employee_type,
        employee_branch,
        start_month_date,
        COUNT(quiz_id) AS quizs,
        COUNT(answer) AS answered
    FROM source
    GROUP BY komu_user_id,hrm_user_id,email,fullname,employee_level,employee_type,employee_branch,start_month_date
),
correct_answer AS (
    SELECT
        komu_user_id,
        start_month_date,
        COUNT(answer) AS corrected_answers 
    FROM source
    WHERE correct = true
    GROUP BY komu_user_id, start_month_date
),
fail_answer AS (
    SELECT 
        komu_user_id,
        start_month_date,
        COUNT(answer) AS failed_answers
    FROM source
    WHERE correct = false
    GROUP BY komu_user_id, start_month_date
),
data_mapped AS (
    SELECT 
        aq.*,
        et.employee_type_code,
        COALESCE(c.corrected_answers, 0) AS corrected_answers,
        COALESCE(f.failed_answers, 0) AS failed_answers
    FROM answer_quiz aq
    LEFT JOIN employee_type et
        ON aq.employee_type = et.employee_type_id
    FULL OUTER JOIN correct_answer c 
        ON aq.komu_user_id = c.komu_user_id
        AND aq.start_month_date = c.start_month_date
    FULL OUTER JOIN fail_answer f
        ON aq.komu_user_id = f.komu_user_id
        AND aq.start_month_date = f.start_month_date
)
SELECT 
    *
FROM 
    data_mapped