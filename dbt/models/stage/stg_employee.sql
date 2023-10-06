{{ config(
    materialized = 'table',
    schema = 'stage'
) }}

WITH hrm_users AS (

    SELECT
        *
    FROM
        {{ ref(
            'base_hrm2_employees'
        ) }}
),
employee_contracts AS (
    SELECT
        employee_id,
        MAX(start_date) AS start_date
    FROM
        {{ ref('base_hrm2_employeecontracts') }}
    GROUP BY
        employee_id
),
prj_users AS (
    SELECT
        *
    FROM
        {{ ref(
            'base_prj_abpusers'
        ) }}
),
tsh_users AS (
    SELECT
        *
    FROM
        {{ ref(
            'base_tsh_abpusers'
        ) }}
),
tsh_mapped_users AS (
    SELECT
        tu.*,
        tu2.email AS manager_email
    FROM
        tsh_users tu
        LEFT JOIN tsh_users tu2
        ON tu.tsh_manager_user_id = tu2.tsh_user_id
),
komu_users AS (
    SELECT
        *
    FROM
        {{ ref(
            'base_komu_users'
        ) }}
),
talent_users AS (
    SELECT
        * 
    FROM 
        {{ref(
            'base_tal_abpusers'
        )}}
),
data_mapped AS (
    SELECT
        hu.*,
        pu."prj_user_id",
        pu."pool_note",
        pu."star_rate",
        pu."avatar_path",
        pu."komu_user_id" AS "komu_user",
        (
            CASE
                WHEN hu."start_working_time" IS NOT NULL THEN hu."start_working_time"
                WHEN ec."start_date" IS NOT NULL THEN ec."start_date"
                ELSE NULL
            END
        ) :: DATE AS "contract_start_date",
        tu."job_title",
        tu."tsh_user_id",
        tu."tsh_manager_user_id",
        tu."manager_email",
        tu."is_to_work",
        tu."morning_end_at",
        tu."afternoon_end_at",
        tu."morning_start_at",
        tu."afternoon_start_at",
        tu."morning_working_time",
        tu."afternoon_working",
        tu."is_working_time_default",
        ku.komu_user_id,
        ku.quiz_score,
        ta.tal_abpuser_id AS talent_user_id
    FROM
        hrm_users hu
        LEFT JOIN prj_users pu
        ON hu.email = pu.email
        LEFT JOIN employee_contracts ec
        ON ec.employee_id = hu.hrm_user_id
        LEFT JOIN tsh_mapped_users tu
        ON tu.email = hu.email
        LEFT JOIN komu_users ku
        ON ku.full_email = hu.email
        LEFT JOIN talent_users ta
        ON ta.email = hu.email
),
data_aggrated AS (
    SELECT
        mu.*,
        (
            CASE
                WHEN "status" = 3 THEN TRUE
                ELSE FALSE
            END
        ) :: BOOLEAN AS "is_quit",
        {{ extract_province_code('issued_by') }} :: VARCHAR AS "province_code",
        {{ extract_province_code('place_of_permanent') }} :: VARCHAR AS "permanent_province_code"
    FROM
        data_mapped mu
)
SELECT
    DISTINCT
    ON (email) *
FROM
    data_aggrated
