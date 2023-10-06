{{ config(
    materialized = 'view',
    schema = 'base'
) }}

WITH source AS (

    SELECT
        _airbyte_data::json->'id' as id,
        _airbyte_data::json->'lang' as lang,
        _airbyte_data::json->'color' as color,
        _airbyte_data::json->'prefix' as prefix,
        _airbyte_data::json->'plugins' as plugins,
        _airbyte_data::json->'announce' as announce,
        _airbyte_data::json->'serverID' as server_id,
        _airbyte_data::json->'protections' as protections,
        _airbyte_data::json->'util_enabled' as util_enabled,
        _airbyte_data::json->'defaultVolume' as defaultvolume,
        _airbyte_data::json->'requestChannel' as requestchannel,
        _airbyte_data::json->'requestMessage' as requestmessage
    FROM
        {{ source(
            'raw_komu',
            '_airbyte_raw_komu_guilddata'
        ) }}

),
data_cleaning AS (
    SELECT
        {{trim_qoutes_col('id')}},
        {{trim_qoutes_col('lang')}},
        {{trim_qoutes_col('color')}},
        {{trim_qoutes_col('prefix')}},
        {{trim_qoutes_col('plugins')}},
        {{trim_qoutes_col('announce')}},
        {{trim_qoutes_col('server_id')}},
        {{trim_qoutes_col('protections')}},
        {{trim_qoutes_col('util_enabled')}},
        {{trim_qoutes_col('defaultvolume')}},
        {{trim_qoutes_col('requestchannel')}},
        {{trim_qoutes_col('requestmessage')}}
    FROM source
),
data_type_rename_conversion AS (
    SELECT
        id :: INT AS guild_data_id,
        lang :: VARCHAR AS lang,
        color :: VARCHAR color,
        prefix :: VARCHAR prefix,
        plugins :: VARCHAR plugins,
        announce :: BOOLEAN AS announce,
        server_id :: VARCHAR AS server_id,
        protections :: VARCHAR protections,
        util_enabled :: BOOLEAN AS util_enabled,
        defaultvolume :: VARCHAR default_volume,
        requestchannel :: VARCHAR request_channel,
        requestmessage :: VARCHAR request_message

    FROM
        data_cleaning
)
SELECT
    *
FROM
    data_type_rename_conversion
