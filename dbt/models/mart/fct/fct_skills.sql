{{ config(
    schema = 'mart',
    materialized = "table"
) }}

WITH dim_skills AS (
    select * from {{ref('dim_skills')}}
),
dim_employee_skills AS (
    select * from {{ref('dim_employee_skills')}}
),
dim_skill_count AS (
    select count(*) as skill_count, des.prj_skill_id 
    from dim_employee_skills des 
    inner join dim_employee_skills des2 
    on des.prj_skill_id = des2.prj_skill_id 
        and des.prj_user_id = des2.prj_user_id
    group by des.prj_skill_id
),
data_maped AS (
    select ds.*, dsc.skill_count from dim_skills ds
    left join dim_skill_count dsc
        on ds.prj_skill_id = dsc.prj_skill_id
),
data_aggreated AS (
    select * from data_maped
)

select *
from data_aggreated