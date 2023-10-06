{{ config(
    schema = 'mart',
    materialized = "table"
) }}

select *
from {{ ref('dim_employee') }} 