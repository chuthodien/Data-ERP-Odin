{{ config(
    materialized = 'table',
    schema = 'stage'
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ ref('base_tal_emailstatushistories') }}
    WHERE
        is_deleted IS FALSE
),
data_type_rename_conversion AS (
    SELECT
        tal_email_status_history_id AS id,
        cv_id,
        tenant_id,
        description,
        creation_time,
        deletion_time,
        creator_user_id,
        deleter_user_id,
        email_template_id,
        last_modifier_user_id,
        last_modification_time
    FROM
        source
)
SELECT
    *
FROM
    data_type_rename_conversion
