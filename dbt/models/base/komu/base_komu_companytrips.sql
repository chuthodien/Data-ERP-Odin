{{ config(
    materialized = 'view',
    schema = 'base'
) }}

WITH source AS (

    SELECT
        _airbyte_data::json->'id' as id,
        _airbyte_data::json->'role' as role,
        _airbyte_data::json->'room' as room,
        _airbyte_data::json->'year' as trip_year,
        _airbyte_data::json->'email' as email,
        _airbyte_data::json->'phone' as phone,
        _airbyte_data::json->'office' as office,
        _airbyte_data::json->'userId' as userid,
        _airbyte_data::json->'fullName' as fullname,
        _airbyte_data::json->'kingOfRoom' as kingofroom
    FROM
        {{ source(
            'raw_komu',
            '_airbyte_raw_komu_companytrip'
        ) }}

),
data_cleaning AS (
    SELECT
        {{trim_qoutes_col('id')}},
        {{trim_qoutes_col('role')}},
        {{trim_qoutes_col('room')}},
        {{trim_qoutes_col('trip_year')}},
        {{trim_qoutes_col('email')}},
        {{trim_qoutes_col('phone')}},
        {{trim_qoutes_col('office')}},
        {{trim_qoutes_col('userid')}},
        {{trim_qoutes_col('fullname')}},
        {{trim_qoutes_col('kingofroom')}}
    FROM source
),
data_type_rename_conversion AS (
    SELECT
        id :: INT AS company_trip_id,
        userid :: VARCHAR user_id,
        role :: VARCHAR user_role,
        room :: VARCHAR room,
        trip_year :: VARCHAR company_trip_year,
        email :: VARCHAR email,
        phone :: VARCHAR phone,
        office :: VARCHAR office,
        fullname :: VARCHAR full_name,
        kingofroom :: VARCHAR kingofroom

    FROM
        data_cleaning
)
SELECT
    *
FROM
    data_type_rename_conversion
