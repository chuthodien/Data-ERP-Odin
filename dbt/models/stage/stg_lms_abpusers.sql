{{ config(
    materialized = 'table',
    schema = 'stage'
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ ref(
            'base_lms_abpusers'
        ) }}
),
data_mapped AS (
    SELECT
        lms_user_id as user_id,
        name as user_last_name,
        title,
        avatar,
        surname as user_surname,
        is_active,
        password,
        status_id
        tenant_id,
        username,
        biography,
        is_deleted,
        language_id,
        timezone_id,
        displayname,
        phone_number,
        email_address,
        creation_time,
        deletion_time,
        creator_user_id,
        deleter_user_id,
        last_login_time,
        security_stamp,
        concurrency_stamp,
        is_email_confirmed,
        access_failed_count,
        lockout_end_date_utc,
        password_reset_code,
        is_two_factor_enabled,
        last_modifier_userid,
        normalized_username,
        authentication_source,
        last_modification_time,
        email_confirmation_code,
        is_phonenumber_confirmed,
        normalized_email_address,
        user_personal_info_view_by_public,
        user_personal_links_view_by_public,
        user_personal_achievement_view_by_public,
        userpersonalcertificationviewbypublic as user_personal_certification_view_by_public
    FROM
        source
)
SELECT
    *
FROM
    data_mapped 
