{{ config(
    materialized = 'view',
    schema = 'base'
) }}

WITH source AS (

    SELECT
        _airbyte_data::json->'id' as id,
        _airbyte_data::json->'daily' as daily,
        _airbyte_data::json->'email' as email,
        _airbyte_data::json->'userid' as userid,
        _airbyte_data::json->'channelid' as channelid,
        _airbyte_data::json->'createdAt' as createdat
    FROM
        {{ source(
            'raw_komu',
            '_airbyte_raw_komu_daily'
        ) }}

),
data_cleaning AS (
    SELECT
        {{trim_qoutes_col('id')}},
        {{trim_qoutes_col('daily')}},
        {{trim_qoutes_col('email')}},
        {{trim_qoutes_col('userid')}},
        {{trim_qoutes_col('channelid')}},
        {{trim_qoutes_col('createdat')}}
    FROM source
),
data_type_rename_conversion AS (
    SELECT
        userid::VARCHAR AS komu_user_id,
        channelid::VARCHAR AS channel_id,
        daily::VARCHAR daily,
        CONCAT(email::VARCHAR,'@ncc.asia') user_email,
        TO_TIMESTAMP(createdat::NUMERIC / 1000)::TIMESTAMP created_at
    FROM
        data_cleaning
)
SELECT
    *
FROM
    data_type_rename_conversion
