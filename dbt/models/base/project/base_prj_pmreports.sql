{{ config(
    materialized = 'view',
    schema = 'base'
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ source(
            'raw_project',
            'prj_pmreports'
        ) }}
),
data_selection AS (
    SELECT
        *
    FROM
        source
),
data_type_rename_conversion AS (
    SELECT
        "Id" :: INT AS "prj_pmreport_id",
        "Name" :: VARCHAR AS "repport_name",
        "note" :: VARCHAR AS "report_note",
        "Type" :: INT AS "report_type",
        "Year" :: INT AS "year",
        "pmreportstatus" :: INT "report_status",
        {{ extract_meta_columns() }}
    FROM
        data_selection
)
SELECT
    *
FROM
    data_type_rename_conversion
