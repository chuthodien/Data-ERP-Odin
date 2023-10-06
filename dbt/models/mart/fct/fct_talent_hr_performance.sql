{{ config(
    materialized = 'table',
    schema = 'mart'
) }}
WITH employee AS (
    SELECT 
        *
    FROM
        {{ref('dim_employee')}}
),
cvs_creation AS (
    SELECT
        creator_user_id,
        create_at_month,
        COUNT(talent_cv_id) AS cvs_creation
    FROM {{ref('dim_talent_cvs')}}
    WHERE creator_user_id IS NOT NULL
    GROUP BY (create_at_month, creator_user_id)
),
cvs_modification AS (
    SELECT
        last_modifier_user_id,
        last_update_at_month,
        COUNT(talent_cv_id) AS cvs_modification
    FROM {{ref('dim_talent_cvs')}}
    WHERE last_modifier_user_id IS NOT NULL
    GROUP BY (last_update_at_month, last_modifier_user_id)
),
emails_status_creation AS (
    SELECT 
        creator_user_id,
        create_at_month,
        COUNT(id) AS emails_status_creation
    FROM {{ref('dim_email_status_history')}}
    WHERE creator_user_id IS NOT NULL
    GROUP BY creator_user_id, create_at_month
),
emails_status_modification AS (
    SELECT 
        last_modifier_user_id,
        last_update_at_month,
        COUNT(id) AS emails_status_modification
    FROM {{ref('dim_email_status_history')}}
    WHERE last_modifier_user_id IS NOT NULL
    GROUP BY last_modifier_user_id, last_update_at_month
),
request_cvs_creation AS (
    SELECT 
        creator_user_id,
        create_at_month,
        COUNT(request_cv_id) AS request_cvs_creation
    FROM {{ref("dim_talent_request_cvs")}}
    WHERE creator_user_id IS NOT NULL
    GROUP BY (creator_user_id, create_at_month)
),
request_cvs_modification AS (
    SELECT 
        last_modifier_user_id,
        last_update_at_month,
        COUNT(request_cv_id) AS request_cvs_modification
    FROM {{ref("dim_talent_request_cvs")}}
    WHERE last_modifier_user_id IS NOT NULL
    GROUP BY (last_modifier_user_id, last_update_at_month)
),
cvs AS (
    SELECT
        COALESCE(creator_user_id, last_modifier_user_id) AS talent_user_id,
        COALESCE(create_at_month, last_update_at_month) AS time_at_month,
        COALESCE(cvs_creation, 0) AS cvs_creation,
        COALESCE(cvs_modification, 0) AS cvs_modification 
    FROM cvs_creation cvsc
    FULL OUTER JOIN cvs_modification cvsm 
        ON creator_user_id = last_modifier_user_id AND create_at_month = last_update_at_month 
),
emails AS (
    SELECT
        COALESCE(creator_user_id, last_modifier_user_id) AS talent_user_id,
        COALESCE(create_at_month, last_update_at_month) AS time_at_month,
        COALESCE(emails_status_creation, 0) AS emails_status_creation,
        COALESCE(emails_status_modification, 0) AS emails_status_modification 
    FROM emails_status_creation esc 
    FULL OUTER JOIN emails_status_modification esm
        ON creator_user_id = last_modifier_user_id AND create_at_month = last_update_at_month
),
request_cvs AS (
    SELECT
        COALESCE(creator_user_id, last_modifier_user_id) AS talent_user_id,
        COALESCE(create_at_month, last_update_at_month) AS time_at_month,
        COALESCE(request_cvs_creation, 0) AS request_cvs_creation,
        COALESCE(request_cvs_modification, 0) AS request_cvs_modification 
    FROM request_cvs_creation rcvc
    FULL OUTER JOIN request_cvs_modification rcvm
        ON creator_user_id = last_modifier_user_id AND create_at_month = last_update_at_month
),
requests AS (
    SELECT
        talent_user_id,
        time_at_month,
        SUM(requests_creation) AS requests_creation,
        SUM(requests_modification) AS requests_modification
    FROM {{ref('fct_performance_requests')}}
    GROUP BY (talent_user_id, time_at_month)
),
data_aggrated AS (
    SELECT  
        COALESCE(cvs.talent_user_id, emails.talent_user_id, requests.talent_user_id, request_cvs.talent_user_id) AS talent_user_id,
        COALESCE(cvs.time_at_month, emails.time_at_month, requests.time_at_month, request_cvs.time_at_month) AS time_at_month,
        COALESCE(cvs_creation, 0) AS cvs_creation,
        COALESCE(cvs_modification, 0) AS cvs_modification,
        COALESCE(emails_status_creation, 0) AS emails_status_creation,
        COALESCE(emails_status_modification, 0) as emails_status_modification,
        COALESCE(requests_creation, 0) AS requests_creation,
        COALESCE(requests_modification, 0) AS requests_modification,
        COALESCE(request_cvs_creation, 0) AS request_cvs_creation,
        COALESCE(request_cvs_modification, 0) AS request_cvs_modification
    FROM cvs 
    FULL OUTER JOIN emails ON cvs.talent_user_id = emails.talent_user_id AND cvs.time_at_month = emails.time_at_month
    FULL OUTER JOIN requests ON requests.talent_user_id = cvs.talent_user_id AND cvs.time_at_month = requests.time_at_month
    FULL OUTER JOIN request_cvs ON request_cvs.talent_user_id = cvs.talent_user_id AND CVS.time_at_month = request_cvs.time_at_month
),
data_mapped AS (
    SELECT 
        employee.email,
        employee.fullname,
        employee.hrm_user_id,
        data_aggrated.*
    FROM data_aggrated 
    INNER JOIN employee ON data_aggrated.talent_user_id = employee.talent_user_id
    ORDER BY time_at_month, talent_user_id
)
SELECT
    * 
FROM 
    data_mapped