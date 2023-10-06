{{ config(
    materialized = 'view',
    schema = 'base'
) }}

WITH source AS (

    SELECT
        _airbyte_data::json->'id' as id,
        _airbyte_data::json->'data' as data,
        _airbyte_data::json->'type' as type,
        _airbyte_data::json->'status' as status,
        _airbyte_data::json->'userId' as userid,
        _airbyte_data::json->'wfhMsg' as wfhmsg,
        _airbyte_data::json->'complain' as complain,
        _airbyte_data::json->'createdAt' as createdat,
        _airbyte_data::json->'pmconfirm' as pmconfirm
    FROM
        {{ source(
            'raw_komu',
            '_airbyte_raw_komu_wfh'
        ) }}

),
data_cleaning AS (
    SELECT
        {{trim_qoutes_col('id')}},
        {{trim_qoutes_col('data')}},
        {{trim_qoutes_col('type')}},
        {{trim_qoutes_col('status')}},
        {{trim_qoutes_col('userid')}},
        {{trim_qoutes_col('wfhmsg')}},
        {{trim_qoutes_col('complain')}},
        {{trim_qoutes_col('createdat')}},
        {{trim_qoutes_col('pmconfirm')}}
    FROM source
),
data_type_rename_conversion AS (
    SELECT
        id :: VARCHAR AS wfh_id,
        userid :: VARCHAR komu_user_id,
        data :: VARCHAR AS wfh_data,
        type :: VARCHAR AS wfh_type,
        status :: VARCHAR AS wfh_status,
        wfhmsg :: VARCHAR AS wfh_message,
        complain :: BOOLEAN AS is_complained,
        pmconfirm :: BOOLEAN AS is_pm_confirmed,
        TO_TIMESTAMP(createdat::NUMERIC / 1000)::TIMESTAMP created_time
    FROM
        data_cleaning
)
SELECT
    *
FROM
    data_type_rename_conversion
