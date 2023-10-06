{{ config(
    materialized = 'table',
    schema = 'stage'
) }}

WITH base_hrm_benefitemployees AS (
    SELECT
        *
    FROM
        {{ref('base_hrm2_benefitemployees')}}
    WHERE 
        is_deleted = false
),
base_hrm_benefits AS (
    SELECT 
        *
    FROM 
        {{ref('base_hrm2_benefits')}}
),
data_mapped AS (
    SELECT 
        be.hrm_benefitemployees_id AS hrm_benefitemployee_id,
        be.benefit_id,
        be.employee_id,
        be.start_date,
        be.end_date,
        be.creation_time,
        be.creatoruser_id,
        be.lastmodifieruser_id,
        be.last_modification_time,
        b.name AS benefit_name,
        b.type AS benefit_type,
        b.money,
        b.is_active,
        b.is_belong_toallemployee AS is_belong_to_all_employee,
        {{dbt_utils.date_trunc('month', "be.start_date")}} AS start_at_month,
        {{dbt_utils.date_trunc('month', "be.end_date")}} AS end_at_month
    FROM base_hrm_benefitemployees be
    INNER JOIN base_hrm_benefits b ON be.benefit_id = b.hrm_benefits_id
)
SELECT 
    *
FROM 
    data_mapped
