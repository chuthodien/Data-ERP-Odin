{{ config(
    materialized = 'table',
    schema = 'base'
) }}

WITH source AS (

    SELECT
        _airbyte_data::json->'id' as id,
        _airbyte_data::json->'title' as title
    FROM
        {{ source(
            'raw_komu',
            '_airbyte_raw_komu_birthdays'
        ) }}

),
data_cleaning AS (
    SELECT
        {{trim_qoutes_col('id')}},
        {{trim_qoutes_col('title')}}
    FROM source
),
data_type_rename_conversion AS (
    SELECT
        id :: INT AS birthday_id,
        title ::VARCHAR title
    FROM
        data_cleaning
)
SELECT
    *
FROM
    data_type_rename_conversion

