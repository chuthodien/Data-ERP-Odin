{{ config(
    materialized = 'table',
    schema = 'mart'
) }}

WITH candidates AS (
    SELECT
        *
    FROM
        {{ref('stg_talent_cvs')}}
),

branches AS (
    SELECT
        *
    FROM
        {{ ref('stg_talent_branches') }}
),

subpositions AS (

    SELECT
        *
    FROM
        {{ ref(
            'stg_talent_sub_positions'
        ) }}
),

positions AS (
    SELECT
        *
    FROM
        {{ ref('stg_talent_positions') }}
),

data_position AS (
    SELECT
        s.*,
        position_name
    FROM
        subpositions s 
        INNER JOIN positions p
        ON s.position_id = p.position_id
) 

SELECT
    cdd.*,
    sub_position_name,
    position_id,
    position_name,
    branch_name,
    branch_address,
    branch_display_name
FROM 
    candidates cdd
    INNER JOIN data_position dp 
    ON cdd.sub_position_id = dp.sub_position_id
    INNER JOIN branches br 
    ON cdd.branch_id =  br.branch_id 