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
            'tsh_reviewdetails'
        ) }}
),
data_type_rename_conversion AS (
    SELECT
        "Id" :: INT AS "tsh_reviewdetails_id",
        "note" :: VARCHAR AS "review_intern_note",
        "Type" :: INT AS "review_intern_type",
        "salary" :: INT AS "salary",
        "status" :: INT AS "review_intern_status",
        "newlevel" :: INT AS "after_level",
        "ratestar" :: INT AS "rate_star",
        "reviewid" :: INT AS "tsh_reviewinterns_id",
        "sublevel" :: INT AS "sub_level",
        "reviewerid" :: INT AS "reviewer_id",
        "currentlevel" :: INT AS "before_level",
        "internshipid" :: INT AS "internship_id",
        "isfullsalary" :: INT AS "is_full_salary",
        {{ extract_meta_columns() }}
    FROM
        source
)
SELECT
    *
FROM
    data_type_rename_conversion
