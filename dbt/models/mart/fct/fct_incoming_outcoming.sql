{{ config(
    schema = 'mart',
    materialized = "table"
) }}
WITH incoming AS (
    SELECT
        *,
        CASE
            WHEN code='Thu từ khách hàng' 
            THEN vnd_value 
            ELSE 0 
        END AS "from_customer"
    FROM {{ref('dim_incomingentries')}}
),
outcoming AS (
    SELECT 
        *,
        CASE 
            WHEN code='SALARY'
            THEN VALUE 
            ELSE 0 
        END AS "salary"
    FROM {{ref('dim_outcomingentries')}}
),	
incoming_month AS (
    SELECT
        SUM(vnd_value) AS "sum_incoming",
        SUM(from_customer) AS "from_customer",
        date_at,
        branch_code
    FROM incoming
    GROUP BY date_at, branch_code
),
outcoming_month AS (
    SELECT
        SUM(value) AS "sum_outcoming",
        SUM(salary) AS "salary",
        date_at,
        branch_code
    FROM outcoming
    WHERE workflow_status_code LIKE 'END'
    AND code not in ('chuyen_nham__test_','chi_chuyen_doi_tai_khoan','Chi chuyển đổi ', 'chi_mo_so_tiet_kiem')
    GROUP BY date_at, branch_code
),
data_mapped AS (
    SELECT
        COALESCE(om.sum_outcoming, 0) AS "outcoming_value",
        im.from_customer,
        om.salary,
        COALESCE(im.date_at, om.date_at) AS "date_at",
        om.branch_code
    FROM incoming_month im
    FULL OUTER JOIN outcoming_month om 
    ON im.date_at= om.date_at AND im.branch_code = om.branch_code
    
)
SELECT
*
FROM data_mapped