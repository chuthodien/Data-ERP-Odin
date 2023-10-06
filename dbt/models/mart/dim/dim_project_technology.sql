{{ config(
    materialized = 'table',
    schema = 'mart'
) }}

WITH stg_project_technologies AS (
    SELECT
        *
    FROM {{ ref('stg_project_technologies')}}
),
stg_technologies AS (
    SELECT
        *
    FROM {{ ref('stg_technologies')}}
),
dim_project AS (
    SELECT
        *
    FROM
        {{ ref('dim_project') }}
),
data_mapped AS (
    SELECT
        spt.prj_projecttechnology_id,
        spt.project_technologies_is_deleted,
        spt.project_technologies_creation_time,
        spt.project_technologies_deletion_time,
        spt.project_technologies_last_modification_time,
        st.technology_name,
        st.technology_color,
        dp.*,
        {{add_date_at('spt.project_technologies_creation_time', 'project_technologies_date_at')}}
    FROM stg_project_technologies spt
    LEFT JOIN stg_technologies st
    ON spt.prj_technology_id = st.prj_technology_id
    LEFT JOIN dim_project dp
    ON spt.prj_project_id = dp.prj_project_id
)
SELECT
    *
FROM
    data_mapped