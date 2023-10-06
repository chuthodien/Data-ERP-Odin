{{ config(
    materialized = 'view',
    schema = 'base'
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ source(
            'raw_timesheet',
            'tsh_timekeepings'
        ) }}
    WHERE
        userid IS NOT NULL
),
data_type_rename_conversion AS (
    SELECT
        "Id",
        {{ to_timestamp("dateat") }} :: TIMESTAMP AS "time_at",
        "userid" :: INT AS "tsh_user_id",
        "islocked" :: BOOLEAN AS "is_locked",
        "usernote" :: text AS "user_note",
        "notereply" :: text AS "note_reply",
        {{ to_timestamp(
            'checkin',
            'HH24:MI'
        ) }} :: TIME AS checkin_time,
        {{ to_timestamp(
            'checkout',
            'HH24:MI'
        ) }} :: TIME AS checkout_time,
        {{ to_timestamp(
            'registercheckin',
            'HH24:MI'
        ) }} :: TIME AS "register_checkin",
        {{ to_timestamp(
            'registercheckout',
            'HH24:MI'
        ) }} :: TIME AS "register_checkout",
        "ispunishedcheckin" :: BOOLEAN AS "is_punished_checkin",
        "ispunishedcheckout" :: BOOLEAN AS "is_punished_checkout",
        {{ extract_meta_columns() }}
    FROM
        source
)
SELECT
    *
FROM
    data_type_rename_conversion
