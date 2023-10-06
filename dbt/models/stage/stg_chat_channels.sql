{{ config(
    materialized = 'table',
    schema = 'stage'
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ ref(
            'base_komu_channels'
        ) }}
),
data_selection AS (
    SELECT
        *
    FROM
        source
),
data_type_rename_conversion AS (
    SELECT
        komu_channel_id,
        channel_name,
        nsfw,
        channel_type,
        raw_position,
        last_message_id,
        rate_limit_per_user
    FROM
        data_selection
)
SELECT
    *
FROM
    data_type_rename_conversion
