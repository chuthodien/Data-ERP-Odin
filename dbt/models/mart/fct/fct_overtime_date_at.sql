{{ config(
    schema = 'mart',
    materialized = "table"
) }}
with overtime AS (
	SELECT *,
	CASE WHEN ov.is_charge=true
	     THEN {{ms_to_hour('working_time')}}::FLOAT 
	     ELSE 0
	END AS "time_charged",
	CASE WHEN ov.is_charge=false 
	     THEN {{ms_to_hour('working_time')}}::FLOAT 
	     ELSE 0 
	END  AS "time_uncharged"
	FROM {{ref('dim_timesheet')}} ov
	WHERE work_type_id =1	
),
 data_mapped AS (
	SELECT 
	tsh_project_code,
	SUM(time_charged) AS "total_charged",
	SUM(time_uncharged) AS "total_uncharged",
	date_at
	FROM overtime
	GROUP BY date_at,tsh_project_code
)
SELECT
    *
FROM 
    data_mapped 