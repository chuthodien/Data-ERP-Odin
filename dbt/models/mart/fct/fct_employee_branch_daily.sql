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
dim_date AS (
    SELECT
        *
    FROM
        {{ ref('dim_date') }}
),
dim_branch AS (
    SELECT
        *
    FROM
        {{ ref('dim_branch') }}
),
data_date_branch AS (
    SELECT
        *
    FROM
        dim_date dd
    LEFT JOIN 
        dim_branch db
    ON dd.date_day is not null
),
data_onboard AS (
    SELECT 
        de.branch_id as branch,
        de.onboard_date,
        COUNT(*) AS total_onboard
    FROM
        dim_employee de 
    GROUP BY 
        de.branch_id,
        de.onboard_date
),
data_offboard AS (
    SELECT 
        de.offboard_date,
        de.branch_id as branch,
        COUNT(*) AS total_offboard
    FROM
        dim_employee de 
    GROUP BY 
        de.branch_id,
        de.offboard_date
),
data_staff_offboard AS (
    SELECT 
        de.offboard_date,
        de.branch_id as branch,
        COUNT(*) AS total_offboard
    FROM
        dim_employee de
    WHERE de.is_intern IS FALSE
    GROUP BY 
        de.branch_id,
        de.offboard_date
),
data_intern_offboard AS (
    SELECT 
        de.branch_id as branch,
        de.offboard_date,
        COUNT(*) AS total_offboard
    FROM
        dim_employee de
    WHERE de.is_intern IS TRUE
    GROUP BY 
        de.branch_id,
        de.offboard_date
),
data_aggrated AS (
    SELECT
        ddb.*,
        ddb.date_day AS date_at,
        COALESCE(don.total_onboard, 0) AS total_onboard,
        COALESCE(doff.total_offboard, 0) AS total_offboard,
        COALESCE(dsoff.total_offboard, 0) AS total_staff_offboard,
        COALESCE(dioff.total_offboard, 0) AS total_intern_offboard,
        (COALESCE(don.total_onboard, 0) - COALESCE(doff.total_offboard, 0)) AS employee_change
    FROM
        data_date_branch ddb
    LEFT JOIN data_onboard don
        ON don.onboard_date = ddb.date_day AND don.branch = ddb.branch_id
    LEFT JOIN data_offboard doff
        ON doff.offboard_date = ddb.date_day AND doff.branch = ddb.branch_id
    LEFT JOIN data_staff_offboard dsoff
        ON dsoff.offboard_date = ddb.date_day AND dsoff.branch = ddb.branch_id
    LEFT JOIN data_intern_offboard dioff
        ON dioff.offboard_date = ddb.date_day AND dioff.branch = ddb.branch_id
)
SELECT
    *
    ,sum(employee_change) over (PARTITION BY branch_id order by date_at) total_employee 
FROM
    data_aggrated