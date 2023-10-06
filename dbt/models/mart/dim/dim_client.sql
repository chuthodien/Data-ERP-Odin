{{ config(
    materialized = 'table',
    schema = 'mart'
) }}

WITH source AS (

    SELECT
        "prj_clients_id",
        "client_code",
        "client_name",
        "client_address",
        "client_is_deleted",
        "client_creation_time",
        "client_deletion_time",
        "client_last_modification_time"     
    FROM
        {{ ref( 'stg_clients' ) }}

)
SELECT
    *
FROM
    source
