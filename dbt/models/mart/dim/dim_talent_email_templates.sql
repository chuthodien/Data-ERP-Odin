{{ config (
    materialized = 'table',
    schema = 'mart'
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ ref('stg_talent_email_templates') }}
),
template_types AS (
    SELECT
        *
    FROM
        {{ ref('email_template_types') }}
),
data_mapped AS (
    SELECT
        source.*,
        template_types.type_code
    FROM
        source
        INNER JOIN template_types
        ON source.type = template_types.type_id
)
SELECT
    *
FROM
    data_mapped
