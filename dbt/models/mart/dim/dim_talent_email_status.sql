{{ config(
    materialized = 'table',
    schema = 'mart'
) }}

WITH email_status_histories AS (

    SELECT
        *
    FROM
        {{ ref('stg_talent_emailstatushistories') }}
),
email_status AS(
    SELECT
        tb1.*
    FROM
        email_status_histories tb1
        INNER JOIN (
            SELECT
                cv_id,
                MAX(creation_time) last_creation_time
            FROM
                email_status_histories
            GROUP BY
                cv_id
        ) tb2
        ON tb1.cv_id = tb2.cv_id
        AND tb1.creation_time = tb2.last_creation_time
),
email_template AS(
    SELECT
        tal_email_template_id AS id,
        NAME,
        TYPE,
        subject,
        type_code AS email_status
    FROM
        {{ ref('dim_talent_email_templates') }}
),
data_mapped AS (
    SELECT
        email_status.id,
        email_status.cv_id,
        email_status.description,
        email_status.creation_time,
        email_status.deletion_time,
        email_status.email_template_id,
        email_status.last_modification_time,
        email_template.email_status
    FROM
        email_status
        INNER JOIN email_template
        ON email_status.email_template_id = email_template.id
)
SELECT
    *
FROM
    data_mapped
