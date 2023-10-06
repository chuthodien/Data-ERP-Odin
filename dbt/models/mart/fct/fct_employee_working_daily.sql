{{ config(
    schema = 'mart',
    materialized = "table"
) }}


SELECT 
	COALESCE(fetd.date_at, fecd.date_at, fead.date_at) as date_at,
    COALESCE(fetd.employee_email, fecd.employee_email, fead.employee_email) AS employee_email,
    COALESCE(fecd.tracked_time, 0) AS tracked_time,
    COALESCE(fecd.registered_time, 0) AS registered_time,
    COALESCE(fecd.extra_time, 0) AS extra_time,
	COALESCE(fetd.approved_time, 0) AS approved_time,
    COALESCE(fetd.submited_time, 0) AS submited_time,
    COALESCE(fetd.rejected_time, 0) AS rejected_time,
    COALESCE(fead.off_time, 0) AS off_time,
    COALESCE(fead.onsite_time, 0) AS onsite_time,
    COALESCE(fead.remote_time, 0) AS remote_time,
    COALESCE(fead.absence_time, 0) AS absence_time
FROM {{ ref('fct_employee_timesheet_daily') }} fetd
FULL OUTER JOIN  {{ ref('fct_employee_checkin_daily')}} fecd
    ON fetd.date_at = fecd.date_at
	    AND	fetd.employee_email = fecd.employee_email
FULL OUTER JOIN  {{ ref('fct_employee_absence_daily')}} fead
    ON fetd.date_at = fead.date_at
	AND fetd.employee_email = fead.employee_email
