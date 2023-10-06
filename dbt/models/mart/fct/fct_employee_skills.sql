{{ config(
    schema = 'mart',
    materialized = "table"
) }}

WITH dim_employee AS (
    SELECT
        *
    FROM
        {{ ref('dim_employee') }}
),
dim_employee_skills AS (
    SELECT
        *
    FROM
        {{ ref('dim_employee_skills') }}
),
dim_date AS (
    SELECT
        *
    FROM
        {{ ref('dim_date') }}
),
data_maped AS (
    SELECT
        desk.*
    FROM
        dim_employee_skills desk
        LEFT JOIN dim_date dd
        ON dd.date_day = desk.date_at
),
data_aggreated AS (
    SELECT
        dm.*,
        (
            SELECT 
                COUNT(*)
            FROM  {{ ref('dim_employee_skills') }} 
            WHERE 
                date_at < dm.date_at 
                AND
                (
                    (employee_skill_is_deleted IS FALSE) 
                    OR
                    (employee_skill_deletion_time > dm.date_at)
                )
        ) As skill_count
    FROM
        data_maped dm
        
)
SELECT
    dg.*
FROM
    data_aggreated dg