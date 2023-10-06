{{ config(
    schema = 'mart',
    materialized = "table"
) }}

WITH dim_employee_absence AS (

    SELECT
        *
    FROM
        {{ ref('dim_employee_absence') }}
),
dim_off AS (
    SELECT
        date_at,
        employee_email,
        SUM(absence_time) AS off_time
    FROM
        dim_employee_absence
    WHERE
        absence_status_code = 'APPROVED'
        AND absence_type_code = 'TIMEOFF'
    GROUP BY
        employee_email,
        date_at
),
dim_onsite AS (
    SELECT
        date_at,
        employee_email,
        SUM(absence_time) AS onsite_time
    FROM
        dim_employee_absence
    WHERE
        absence_status_code = 'APPROVED'
        AND absence_type_code = 'ONSITE'
    GROUP BY
        employee_email,
        date_at
),
dim_remote AS (
    SELECT
        date_at,
        employee_email,
        SUM(absence_time) AS remote_time
    FROM
        dim_employee_absence
    WHERE
        absence_status_code = 'APPROVED'
        AND absence_type_code = 'REMOTE'
    GROUP BY
        employee_email,
        date_at
),
data_mapped AS (
    SELECT
        COALESCE(
            dof.date_at,
            dos.date_at,
            drm.date_at
        ) AS date_at,
        COALESCE(
            dof.employee_email,
            dos.employee_email,
            drm.employee_email
        ) AS employee_email,
        COALESCE(
            dof.off_time,
            0
        ) AS off_time,
        COALESCE(
            dos.onsite_time,
            0
        ) AS onsite_time,
        COALESCE(
            drm.remote_time,
            0
        ) AS remote_time
    FROM
        dim_off dof full
        OUTER JOIN dim_onsite dos
        ON dof.employee_email = dos.employee_email
        AND dof.date_at = dos.date_at full
        OUTER JOIN dim_remote drm
        ON dof.employee_email = drm.employee_email
        AND dof.date_at = drm.date_at
),
data_aggrated AS (
    SELECT
        *,
        (
            off_time + onsite_time + remote_time
        ) AS absence_time
    FROM
        data_mapped
)
SELECT
    *
FROM
    data_aggrated
