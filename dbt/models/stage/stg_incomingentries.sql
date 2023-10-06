{{ config(
    materialized = 'table',
    schema = 'stage'
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ ref('base_fin_incomingentries') }}
    WHERE is_deleted =false

),
data_type AS (
    SELECT
        *
    FROM
        {{ ref('base_fin_incomingentrytypes') }}	
),
convert AS (
    SELECT
        *
    FROM
        {{ref('base_fin_currencyconverts')}}
),
branchs AS (
    SELECT 
        *
    FROM
        {{ref('base_fin_branches')}}
),
data_mapped AS (
    SELECT
        s.*,
        t.code,
        t.level,
        cv.Value AS "value_convert",
        s.value * cv.Value AS "vnd_value",
        COALESCE(branchs.code, 'General') AS branch_code,
        date_trunc('MONTH',s.creation_time:: timestamp)::DATE AS "date_at"
    FROM source s
    JOIN data_type t
    ON s.incoming_entry_typeid  = t.id
    JOIN convert cv 
    ON s.currency_id = cv.id
    LEFT JOIN branchs 
    ON branchs.branch_id = s.branch_id
)
    SELECT
        *
    FROM
        data_mapped

