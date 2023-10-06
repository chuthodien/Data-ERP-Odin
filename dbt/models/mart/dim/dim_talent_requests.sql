{{ config(
    materialized = 'table',
    schema = 'mart'
) }}

WITH requests AS (
    SELECT
        *,
        {{ add_start_of_month("creation_time") }},
        {{dbt_utils.date_trunc('month', "creation_time")}} AS create_at_month,
        {{dbt_utils.date_trunc('month', "last_modification_time")}} AS last_update_at_month
    FROM
        {{ ref('stg_talent_requests') }}
),
sub_positions AS (
    SELECT
        *
    FROM
        {{ ref('stg_talent_sub_positions') }}
),
user_types AS(
    SELECT
        user_type_id AS id,
        user_type_code AS code
    FROM
        {{ ref('talent_user_types') }}
),
request_levels AS(
    SELECT
        *
    FROM
        {{ ref('request_levels') }}
),
request_priority AS (
    SELECT
        *
    FROM
        {{ ref('request_priority') }}
),
data_mapped AS(
    SELECT
        requests.request_id,
        "level" AS request_level,
        (
            CASE
                WHEN rl.request_level_id IS NOT NULL THEN rl.request_level_code
                ELSE 'Uncategorized'
            END
        ) :: VARCHAR AS request_level_code,
        (
            CASE
                WHEN "status" = 0 THEN 'InProgress'
                WHEN "status" = 1 THEN 'Closed'
                ELSE 'Uncategorized'
            END
        ) :: VARCHAR AS status_code,
        "status",
        (
            CASE
                WHEN sp.sub_position_id IS NOT NULL THEN sp.sub_position_name
                ELSE 'Uncategorized'
            END
        ) :: VARCHAR AS sub_position_name,
        (
            CASE
                WHEN user_types.id IS NOT NULL THEN user_types.code
                ELSE 'Uncatergorized'
            END
        ) :: VARCHAR AS user_type,
        requests.time_need,
        requests.quantity,
        requests.priority,
        rp.request_priority_code,
        requests.creation_time,
        requests.start_of_month,
        requests.deletion_time,
        requests.last_modification_time,
        requests.creator_user_id,
        requests.last_modifier_user_id,
        requests.create_at_month,
        requests.last_update_at_month
    FROM
        requests
        LEFT JOIN sub_positions sp
        ON requests.sub_position_id = sp.sub_position_id
        LEFT JOIN user_types
        ON requests.user_type = user_types.id
        LEFT JOIN request_levels rl
        ON requests.level = rl.request_level_id
        LEFT JOIN request_priority rp
        ON requests.priority = rp.request_priority_id
)
SELECT
    *
FROM
    data_mapped
