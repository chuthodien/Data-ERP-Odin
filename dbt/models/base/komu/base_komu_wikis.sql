{{ config(
    materialized = 'view',
    schema = 'base'
) }}

WITH source AS (

    SELECT
        _airbyte_data::json->'id' as id,
        _airbyte_data::json->'name' as name,
        _airbyte_data::json->'type' as type,
        _airbyte_data::json->'value' as value,
        _airbyte_data::json->'creator' as creator
    FROM
        {{ source(
            'raw_komu',
            '_airbyte_raw_komu_wiki'
        ) }}

),
data_cleaning AS (
    SELECT
        {{trim_qoutes_col('id')}},
        {{trim_qoutes_col('name')}},
        {{trim_qoutes_col('type')}},
        {{trim_qoutes_col('value')}},
        {{trim_qoutes_col('creator')}}
    FROM source
),
data_type_rename_conversion AS (
    SELECT
        id :: VARCHAR AS wiki_id,
        name :: VARCHAR wiki_name,
        type ::VARCHAR wiki_type,
        value :: VARCHAR wiki_value,
        creator ::VARCHAR creator_user_name

    FROM
        data_cleaning
)
SELECT
    *
FROM
    data_type_rename_conversion
