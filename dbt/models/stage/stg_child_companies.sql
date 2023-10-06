{{ config(
    materialized = 'table',
    schema = 'stage'
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ ref(
            'base_hrm_childcompany'
        ) }}
),
data_type_rename_conversion AS (
    SELECT
        "hrm_bonus_id",
        "code",
        "name",
        "is_deleted",
        "creation_time",
        "deletion_time",
        "last_modification_time"
    FROM
        source
)
SELECT
    *
FROM
    data_type_rename_conversion
