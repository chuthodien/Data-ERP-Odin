{{ config(
    schema = 'stage',
    materialized = "table"
) }}
WITH review_details AS (
    SELECT 
        *
    FROM 
        {{ref('base_tsh_reviewdetails')}}
    WHERE is_deleted = false
),
review_intern AS (
    SELECT 
        *
    FROM 
        {{ref('base_tsh_reviewinterns')}}
    WHERE is_deleted = false
),
data_mapped AS (
    SELECT 
        rd.*,
        ri.year,
        ri.month,
        ri.is_active
    FROM review_details rd
    JOIN review_intern ri ON rd.tsh_reviewinterns_id = ri.tsh_reviewinterns_id
)
SELECT 
    *
FROM 
    data_mapped
