{{ config(
    materialized = 'table',
    schema = 'stage'
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ ref('base_tal_abpusers') }}
    WHERE
        is_deleted IS FALSE
),
data_type_rename_conversion AS(
    SELECT
        tal_abpuser_id AS talent_user_id,
        lastname,
        surname,
        address,
        branch AS branch_id,
        is_active,
        username,
        avatar_path,
        position_id,
        phone_number,
        certificates,
        creation_time,
        deletion_time,
        email,
        creator_user_id,
        deleter_user_id,
        is_email_confirmed,
        is_lockout_enabled,
        signaturecontact,
        access_failed_count,
        lockout_end_date_utc,
        personal_attribute,
        is_two_factor_enabled,
        last_modifier_user_id,
        normalized_username,
        normalized_emailaddress,
        authentication_source,
        last_modification_time,
        is_phonenumber_confirmed,
        email_confirmation_code
    FROM
        source
)
SELECT
    *
FROM
    data_type_rename_conversion
