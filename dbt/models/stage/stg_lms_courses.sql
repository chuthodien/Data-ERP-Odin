{{ config(
    materialized = 'table',
    schema = 'stage'
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ ref(
            'base_lms_courses'
        ) }}
),
data_mapped AS (
    SELECT
        lms_courses_id AS course_id,
        "name" as course_name,
        "Type" as type,
        state,
        sourse,
        level_id,
        "Version" as Version,
        syllabus,
        tenant_id,
        is_deleted,
        identifier,
        image_cover,
        language_id,
        sourse_path,
        description,
        creation_time,
        deletion_time,
        related_image,
        creator_user_id,
        deleter_user_id,
        last_modifier_user_id,
        related_information,
        last_modification_time,
        restrictstudentfromv__hiscourseafterenddate,
        studentcanonlypartic__oursebetweenthesedate,
        restrictstudentsfrom__iscoursebeforeenddate
    FROM
        source
)
SELECT
    *
FROM
    data_mapped
