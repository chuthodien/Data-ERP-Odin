{{ config(
    materialized = 'view',
    schema = 'base'
) }}

WITH source AS (

    SELECT
        _airbyte_data::json->'id' as id,
        _airbyte_data::json->'status' as status,
        _airbyte_data::json->'userId' as userid,
        _airbyte_data::json->'end_time' as end_time,
        _airbyte_data::json->'channelId' as channelid,
        _airbyte_data::json->'start_time' as start_time
    FROM
        {{ source(
            'raw_komu',
            '_airbyte_raw_komu_joincall'
        ) }}

),
data_cleaning AS (
    SELECT
        {{trim_qoutes_col('id')}},
        {{trim_qoutes_col('status')}},
        {{trim_qoutes_col('userid')}},
        {{trim_qoutes_col('end_time')}},
        {{trim_qoutes_col('channelid')}},
        {{trim_qoutes_col('start_time')}}
    FROM source
),
data_type_rename_conversion AS (
    SELECT
        status::VARCHAR AS call_status,
        userid::VARCHAR komu_user_id,
        TO_TIMESTAMP(end_time::NUMERIC / 1000)::TIMESTAMP end_time,
        channelid::VARCHAR channel_id,
        TO_TIMESTAMP(start_time::NUMERIC / 1000)::TIMESTAMP start_time,
        CAST(TO_TIMESTAMP(start_time::NUMERIC / 1000)::TIMESTAMP AS TIME)::TIME AS start_time_normalized,
        CAST(TO_TIMESTAMP(end_time::NUMERIC / 1000)::TIMESTAMP AS TIME)::TIME AS end_time_normalized
    FROM
        data_cleaning
)
SELECT
    *
FROM
    data_type_rename_conversion
