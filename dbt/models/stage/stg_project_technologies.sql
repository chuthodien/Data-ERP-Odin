{{ config(
    materialized = 'table',
    schema = 'stage'
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ ref(
            'base_prj_projecttechnologies'
        ) }}
),
data_type_rename_conversion AS (
    SELECT
        "prj_projecttechnology_id",
        "prj_project_id",
        "prj_technology_id",
        {{add_meta_columns('project_technologies_')}}
    FROM
        source
)
SELECT
    *
FROM
    data_type_rename_conversion
