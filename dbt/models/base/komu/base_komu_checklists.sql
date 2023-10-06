{{ config(
    materialized = 'view',
    schema = 'base'
) }}

WITH source AS (

    SELECT
        _airbyte_data::json->'id' as id,
        _airbyte_data::json->'category' as category,
        _airbyte_data::json->'subcategory' as subcategory
    FROM
        {{ source(
            'raw_komu',
            '_airbyte_raw_komu_checklist'
        ) }}

),
data_cleaning AS (
    SELECT
        {{trim_qoutes_col('id')}},
        {{trim_qoutes_col('category')}},
        {{trim_qoutes_col('subcategory')}}
    FROM source
),
data_type_rename_conversion AS (
    SELECT
        id :: INT AS check_list_id,
        category :: VARCHAR check_list_category,
        subcategory :: VARCHAR check_list_sub_category
    FROM
        data_cleaning
)
SELECT
    *
FROM
    data_type_rename_conversion
