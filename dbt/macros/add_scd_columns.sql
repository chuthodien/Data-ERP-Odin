{% macro add_scd_columns() %}
    "_airbyte_active_row" :: VARCHAR AS "active_row_id",
    "_airbyte_unique_key" :: VARCHAR AS "unique_key",
    "_airbyte_unique_key_scd" :: VARCHAR AS "unique_key_scd"
{% endmacro %}
