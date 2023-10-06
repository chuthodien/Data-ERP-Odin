{{ config(
    materialized = 'table',
    schema = 'mart'
) }}

WITH stg_email_status_history AS (
    SELECT
        *
    FROM
        {{ ref('stg_talent_emailstatushistories') }}
),
data_type_rename_conversation AS (
    SELECT
        *,
        {{dbt_utils.date_trunc('month', "creation_time")}} AS create_at_month,
        {{dbt_utils.date_trunc('month', "last_modification_time")}} AS last_update_at_month
    FROM
        stg_email_status_history
)
SELECT
    *
FROM
    data_type_rename_conversation
