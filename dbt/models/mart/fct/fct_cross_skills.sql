{{ config(
    schema = 'mart',
    materialized = "table"
) }}

with dim_skills as (
    select * from {{ ref('dim_skills') }}
),
dim_employee_skills as (
    select * from {{ ref('dim_employee_skills') }}
),
fct_employee as (
    select * from {{ ref('fct_employee') }}
),
skill_metric as (
    select
        ds1.skill_name as skill_name_1,
        ds2.skill_name as skill_name_2,
        ds1.prj_skill_id as metric_skill_id_1,
        ds2.prj_skill_id as metric_skill_id_2
    from
        dim_skills ds1
    cross join dim_skills ds2
    where
        ds1.prj_skill_id <> ds2.prj_skill_id
),
employe_skill_links as (
    select 
        (case when des1.date_at >  des2.date_at then des1.date_at  else  des2.date_at end) as date_at,
        des1.employee_email as employee_email,
        des1.prj_skill_id as prj_skill_id1,
        des2.prj_skill_id as prj_skill_id2
    from
        dim_employee_skills des1
    left join dim_employee_skills des2
		on
		des1.employee_email = des2.employee_email
),
dada_maped as (
    select distinct
        fe.*,
        esl.date_at  as date_at,   
        esl.employee_email as employee_email,
        esl.prj_skill_id1 as prj_skill_id1,
        esl.prj_skill_id2 as prj_skill_id2,
        sm.skill_name_1 as skill_name_1,
        sm.skill_name_2 as skill_name_2
    from
        skill_metric sm
    left join employe_skill_links esl
            on
        sm.metric_skill_id_1 = esl.prj_skill_id1
        and sm.metric_skill_id_2 = esl.prj_skill_id2
    left join fct_employee fe
        on
        fe.email = esl.employee_email
)
select
	*
from
	dada_maped
where 
employee_email is not null
and prj_skill_id1 is not null
and prj_skill_id2 is not null