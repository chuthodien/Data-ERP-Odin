{{ config(
    materialized = 'table',
    schema = 'mart'
) }}

WITH tsh_abpusers AS (

    SELECT
        *
    FROM
        {{ ref(
            'stg_tsh_abpusers'
        ) }}
),

manager_user_not_null AS (
    SELECT
        *
    FROM
        {{ ref(
            'stg_tsh_abpusers'
        ) }}
    WHERE
        manager_user_id is NOT NULL
),

manager_user AS (
    SELECT
        munn.*,
        ta.user_first_name as manager_user_first_name,
        ta.user_last_name as manager_user_last_name,
        ta.username as manager_username
    FROM
        tsh_abpusers ta 
        RIGHT JOIN manager_user_not_null munn
        ON munn.manager_user_id = ta.user_id
)

SELECT
    *
FROM
    manager_user
