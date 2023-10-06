{{ config(
    materialized = 'table',
    schema = 'stage'
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ ref('base_tal_requestcvstatushistories') }}
    WHERE
        is_deleted IS FALSE
),
data_type_rename_conversion AS(
    SELECT
        tal_request_cv_status_history_id AS request_cv_status_history_id,
        status,
        time_at,
        request_cv_id,
        creation_time,
        deletion_time,
        creator_user_id,
        deleter_user_id,
        last_modifier_user_id,
        last_modification_time
    FROM
        source
)
SELECT
    *
FROM
    data_type_rename_conversion
