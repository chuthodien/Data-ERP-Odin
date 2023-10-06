
{{ config(
    materialized = 'table',
    schema = 'mart'
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ ref(
            'stg_day_off_settings',
        ) }}


)
SELECT
    *,
    {{add_date_at('day_off')}}
FROM
    source