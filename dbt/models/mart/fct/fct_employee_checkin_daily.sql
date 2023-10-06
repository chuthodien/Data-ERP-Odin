{{ config(
    schema = 'mart',
    materialized = "table"
) }}

WITH dim_timekeepings AS (
    SELECT
        *,
        {{ time_diff_ms(
            'checkin_time',
            'checkout_time'
        ) }} :: INT AS tracked_time,
        {{ time_diff_ms(
            "register_checkin",
            "register_checkout",
        ) }} :: INT AS registered_time
    FROM
        {{ ref('dim_timekeepings') }}
),
dim_date AS (
    SELECT
        *
    FROM
        {{ ref('dim_date') }}
)
SELECT
    dt.*,
    (
        CASE
        WHEN dt.is_punished_checkin
        THEN register_checkin
        ELSE NULL
        END
    )::TIME AS punished_checkin,
    (
        CASE
        WHEN dt.is_punished_checkout
        THEN register_checkout
        ELSE NULL
        END
    )::TIME AS punished_checkout,
    (
        tracked_time - registered_time
    ) :: INT AS extra_time
FROM
    dim_timekeepings dt