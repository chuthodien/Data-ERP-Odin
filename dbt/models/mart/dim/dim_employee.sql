{{ config(
    schema = 'mart',
    materialized = "table"
) }}

WITH stg_employee AS (

    SELECT
        *
    FROM
        {{ ref('stg_employee') }}
),
employee_extend AS (
    SELECT
        e.*,
        (
            CASE
                WHEN start_working_time IS NOT NULL
                AND contract_start_date IS NOT NULL
                AND start_working_time > contract_start_date THEN {{ dbt_utils.date_trunc(
                    'day',
                    'contract_start_date'
                ) }}
                ELSE {{ dbt_utils.date_trunc(
                    'day',
                    'start_working_time'
                ) }}
            END
        ) :: DATE start_working_date,
        (
            EXTRACT(
                epoch
                FROM(now() - "contract_start_date")) / 31536000
            ) AS employment_period,
            {{ dbt_utils.datediff(
                "e.birthday",
                "now()",
                'year'
            ) }} :: INT AS age
            FROM
                stg_employee e
        ),
        data_mapped AS (
            SELECT
                de.*,
                db.*,
                gc.*,
                et.*,
                jp.job_position_code,
                jp.job_position_name,
                es.*,
                el.level_code AS employee_level_code,
                el.level_name AS employee_level_name,
                el.employee_rank_name
            FROM
                employee_extend de
                LEFT JOIN {{ ref('dim_branch') }}
                db
                ON de.branch = db.branch_id
                LEFT JOIN {{ ref('gender_codes') }}
                gc
                ON de.sex = gc.gender_id
                LEFT JOIN {{ ref('employee_types') }}
                et
                ON de.employee_type = et.employee_type_id
                LEFT JOIN {{ ref('stg_job_positions') }}
                jp
                ON de.job_position_id = jp.job_position_id
                LEFT JOIN {{ ref('employee_status') }}
                es
                ON de.status = es.employee_status_id
                LEFT JOIN {{ ref('stg_employee_levels') }}
                el
                ON de.employee_level = el.level_id
        ),
        data_aggrated AS (
            SELECT
                *,
                employment_period :: INT AS employment_period_year,
                (
                    CASE
                        WHEN employee_type_code = 'INTERN' THEN TRUE
                        ELSE FALSE
                    END
                ) :: BOOLEAN AS is_intern,(
                    CASE
                        WHEN job_position_code = 'dev' THEN 'Dev'
                        WHEN job_position_code = 'tester' THEN 'Tester'
                        ELSE 'Other'
                    END
                ) :: VARCHAR AS job_position_group,
                (
                    CASE
                        WHEN "start_working_date" IS NOT NULL THEN start_working_date
                        ELSE NULL
                    END
                ) :: TIMESTAMP AS onboard_date,
                (
                    CASE
                        WHEN "is_quit" IS TRUE THEN (
                            SELECT
                                {{ dbt_utils.date_trunc(
                                    'day',
                                    'date_at'
                                ) }}
                            FROM
                                {{ ref('stg_employee_workinghistories') }}
                                ewh
                            WHERE
                                data_mapped.hrm_user_id = ewh.employee_id
                                AND ewh.status = 3
                            LIMIT 1
                        )
                        ELSE NULL
                    END
                ) :: TIMESTAMP AS offboard_date,
                (
                    CASE
                        WHEN employment_period > 5 THEN '5 Year'
                        WHEN employment_period > 3 THEN '3 Year'
                        WHEN employment_period > 2 THEN '2 Year'
                        WHEN employment_period > 1 THEN '1 Year'
                        WHEN employment_period > 0.5 THEN '6 Months'
                        ELSE 'Undefined'
                    END
                ) :: VARCHAR AS employment_period_group
            FROM
                data_mapped
        )
    SELECT
        *
    FROM
        data_aggrated
