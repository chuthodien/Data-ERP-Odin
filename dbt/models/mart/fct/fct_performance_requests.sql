{{ config(
    materialized = 'table',
    schema = 'mart'
) }}
WITH requests_creation AS (
    SELECT
        creator_user_id,
        create_at_month,
        COUNT(request_id) AS requests_creation,
        user_type AS requests_creation_user_type
    FROM {{ref('dim_talent_requests')}}
    WHERE creator_user_id IS NOT NULL
    GROUP BY (creator_user_id, create_at_month, user_type)
),
requests_modification AS (
    SELECT
        last_modifier_user_id,
        last_update_at_month,
        COUNT(request_id) AS requests_modification,
        user_type AS requests_modification_user_type
    FROM {{ref('dim_talent_requests')}}
    WHERE last_modifier_user_id IS NOT NULL
    GROUP BY (last_modifier_user_id, last_update_at_month, user_type)
),
requests AS (
    SELECT
        COALESCE(creator_user_id, last_modifier_user_id) AS talent_user_id,
        COALESCE(create_at_month, last_update_at_month) AS time_at_month,
        COALESCE(requests_creation, 0) AS requests_creation,
        COALESCE(requests_modification, 0) AS requests_modification,
        COALESCE(requests_creation_user_type, requests_modification_user_type) AS user_type
    FROM requests_creation reqc
    FULL OUTER JOIN requests_modification reqm
        ON creator_user_id = last_modifier_user_id AND create_at_month = last_update_at_month
)
SELECT 
    *
FROM 
    requests