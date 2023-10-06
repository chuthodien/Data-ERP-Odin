{{ config(
    materialized = 'table',
    schema = 'stage'
) }}

WITH payrolls AS (

    SELECT
        *
    FROM
        {{ ref('base_hrm2_payrolls') }}
),
payslips AS (
    SELECT
        *
    FROM
        {{ ref('base_hrm2_payslips') }}
    WHERE
        is_deleted IS FALSE
),
salarychangerequestemployees AS (
    SELECT
        *
    FROM
        {{ ref('base_hrm2_salarychangerequestemployees') }}
    WHERE
        is_deleted IS FALSE
),
base_hrm_levels AS (
    SELECT
        *
    FROM
        {{ ref('base_hrm2_levels') }}
    WHERE 
        is_deleted = false
),
base_hrm_branches AS (
    SELECT
        *
    FROM
        {{ref('base_hrm2_branches')}}
),
data_mapped AS (
    SELECT
        DISTINCT da.employee_id, 
        da.user_level, 
        da.branch,
        da.apply_month,
        LAST_VALUE (to_salary) over (
            PARTITION BY da.employee_id,
            da.apply_month
            ORDER BY
                da.apply_date ASC RANGE BETWEEN unbounded preceding
                AND unbounded following
        ) AS to_salary
    FROM
        (
            SELECT
                DISTINCT bhp2.employee_id, 
                bhp2.level_id AS user_level, 
                bhp2.branch_id AS branch,
                bhp.apply_month,
                cr.to_salary,
                cr.apply_date
            FROM
                payrolls bhp
                JOIN payslips bhp2
                ON bhp.hrm_payrolls_id = bhp2.payroll_id
                JOIN salarychangerequestemployees cr
                ON bhp2.employee_id = cr.employee_id
            WHERE
                cr.apply_date < bhp.apply_month
                AND cr.is_deleted IS FALSE
            ORDER BY
                bhp.apply_month DESC,
                cr.apply_date DESC
        ) AS da
    ORDER BY
        da.apply_month DESC
)
SELECT
    data_mapped.*,
    {{dbt_utils.date_trunc('month', "data_mapped.apply_month")}} AS time_at_month,
    l.name AS employee_level_code,
    b.name AS branch_name
FROM data_mapped
LEFT JOIN base_hrm_levels l
ON data_mapped.user_level = l.hrm_levels_id
LEFT JOIN base_hrm_branches b
ON data_mapped.branch = b.hrm_branches_id
ORDER BY employee_id, apply_month
