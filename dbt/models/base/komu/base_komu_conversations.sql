{{ config(
    materialized = 'view',
    schema = 'base'
) }}

WITH source AS (

    SELECT
        _airbyte_data::json->'id' as id,
        _airbyte_data::json->'authorId' as authorid,
        _airbyte_data::json->'channelId' as channelid,
        _airbyte_data::json->'createdTimestamp' as createdtimestamp,
        _airbyte_data::json->'past_user_inputs' as pastuserinputs,
        _airbyte_data::json->'updatedTimestamp' as updatedtimestamp,
        _airbyte_data::json->'generated_responses' as generatedresponses
    FROM
        {{ source(
            'raw_komu',
            '_airbyte_raw_komu_conversation'
        ) }}

),
data_cleaning AS (
    SELECT
        {{trim_qoutes_col('id')}},
        {{trim_qoutes_col('authorid')}},
        {{trim_qoutes_col('channelid')}},
        {{trim_qoutes_col('createdtimestamp')}},
        {{trim_qoutes_col('pastuserinputs')}},
        {{trim_qoutes_col('updatedtimestamp')}},
        {{trim_qoutes_col('generatedresponses')}}

    FROM source
),
data_type_rename_conversion AS (
    SELECT
        authorid::VARCHAR AS author_id,
        channelid::VARCHAR AS channel_id,
        pastuserinputs::VARCHAR AS past_user_inputs,
        generatedresponses::VARCHAR AS generated_responses,
        TO_TIMESTAMP(createdtimestamp::NUMERIC / 1000)::TIMESTAMP created_time,
        TO_TIMESTAMP(updatedtimestamp::NUMERIC / 1000)::TIMESTAMP updated_time
    FROM
        data_cleaning
)
SELECT
    *
FROM
    data_type_rename_conversion
