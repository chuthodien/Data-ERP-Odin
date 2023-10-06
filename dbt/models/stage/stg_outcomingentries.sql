{{ config(
    materialized = 'table',
    schema = 'stage'
) }}

WITH source AS (
    SELECT
        *
    FROM
        {{ ref('base_fin_outcomingentries') }}
    WHERE is_deleted =false
),
workflow_status AS (
    SELECT 
        *
    FROM 
        {{ref('base_fin_workflowstatuses')}}
),
branchs AS (
    SELECT 
        *
    FROM
        {{ref('base_fin_branches')}}
),
data_type AS (
    SELECT
        *
    FROM
        {{ ref('base_fin_outcomingentrytypes') }}	
),
data_mapped AS (
    SELECT
        s.*,
        ws.code AS workflow_status_code,
        COALESCE(branchs.code, 'General') AS branch_code,
        t.code,
        t.level,
        date_trunc('MONTH',s.creation_time:: timestamp)::DATE AS "date_at"
    FROM source s
    JOIN data_type t
    ON s.outcoming_entry_typeid  = t.id
    JOIN workflow_status ws
    ON ws.workflow_status_id = s.workflow_status_id
    LEFT JOIN branchs 
    ON branchs.branch_id = s.branch_id
)
    SELECT
        *
    FROM
        data_mapped

