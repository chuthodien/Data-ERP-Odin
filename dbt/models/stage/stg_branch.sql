{{ config(
    schema = 'stage',
    materialized = "table"
) }}

SELECT
    hrm_branches_id AS branch_id,
    code AS branch_code,
    NAME AS branch_name,
    color,
    address AS branch_address,
    is_deleted AS branch_is_deleted,
    short_name AS branch_short_name
FROM
    {{ ref('base_hrm2_branches') }}
