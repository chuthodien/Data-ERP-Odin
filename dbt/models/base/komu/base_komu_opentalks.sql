{{ config(
    materialized = 'view',
    schema = 'base'
) }}

WITH source AS(

    SELECT
        _airbyte_data :: json -> 'id' AS id,
        _airbyte_data :: json -> 'userId' AS userid,
        _airbyte_data :: json -> 'username' AS username,
        _airbyte_data :: json -> 'createdTimestamp' AS created_timestamp
    FROM
        {{ source(
            'raw_komu',
            '_airbyte_raw_komu_opentalks'
        ) }}
),
data_cleaning AS(
    SELECT
        {{ trim_qoutes_col('id') }},
        {{ trim_qoutes_col('userid') }},
        {{ trim_qoutes_col('username') }},
        {{ trim_qoutes_col('created_timestamp') }}
    FROM
        source
),
data_type_rename_conversion AS(
    SELECT
        id :: INT AS "komu_opentalk_id",
        userid :: VARCHAR AS "komu_user_id",
        username,
        TO_TIMESTAMP(
            created_timestamp :: numeric / 1000
        ) :: TIMESTAMP AS creation_time
    FROM
        data_cleaning
)
SELECT
    *
FROM
    data_type_rename_conversion
