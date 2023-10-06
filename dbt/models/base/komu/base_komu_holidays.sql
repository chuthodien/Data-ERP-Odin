{{ config(
    materialized = 'view',
    schema = 'base'
) }}

WITH source AS (

    SELECT
        _airbyte_data::json->'id' as id,
        _airbyte_data::json->'content' as content,
        _airbyte_data::json->'dateTime' as datetime
    FROM
        {{ source(
            'raw_komu',
            '_airbyte_raw_komu_holiday'
        ) }}

),
data_cleaning AS (
    SELECT
        {{trim_qoutes_col('id')}},
        {{trim_qoutes_col('content')}},
        {{trim_qoutes_col('datetime')}}
    FROM source
),
data_type_rename_conversion AS (
    SELECT
        id :: INT AS holiday_id,
        content :: VARCHAR holiday_content,
        datetime :: VARCHAR date_time
    FROM
        data_cleaning
)
SELECT
    *
FROM
    data_type_rename_conversion
