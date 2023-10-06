{{ config (
    materialized = 'view',
    schema = 'base'
) }}

WITH source AS (

    SELECT
        _airbyte_data :: json -> 'id' AS id,
        _airbyte_data :: json -> 'wfh' AS wfh,
        _airbyte_data :: json -> 'date' AS DATE,
        _airbyte_data :: json -> 'email' AS email,
        _airbyte_data :: json -> 'call_time' AS call_time,
        _airbyte_data :: json -> 'spent_time' AS spent_time
    FROM
        {{ source(
            'raw_komu',
            '_airbyte_raw_komu_tracker_spent_time'
        ) }}
),
data_cleaning AS (
    SELECT
        {{ trim_qoutes_col('id') }},
        {{ trim_qoutes_col('wfh') }},
        {{ trim_qoutes_col('date') }},
        {{ trim_qoutes_col('email') }},
        {{ trim_qoutes_col('call_time') }},
        {{ trim_qoutes_col('spent_time') }}
    FROM
        source
),
data_type_rename_conversion AS (
    SELECT
        id :: INT AS komu_tracker_spent_time_id,
        wfh :: BOOLEAN AS wfh,
        DATE :: TIMESTAMP AS creation_time,
        email,
        call_time,
        spent_time
    FROM
        data_cleaning
)
SELECT
    *
FROM
    data_type_rename_conversion
