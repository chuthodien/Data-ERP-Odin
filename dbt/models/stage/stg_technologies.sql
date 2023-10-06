{{ config(
    materialized = 'table',
    schema = 'stage'
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ ref(
            'base_prj_technologies'
        ) }}
),
data_type_rename_conversion AS (
    SELECT
        "prj_technology_id",
        "technology_name",
        "technology_color",
        {{add_meta_columns('technology_')}}
    FROM
        source
)
SELECT
    *
FROM
    data_type_rename_conversion
