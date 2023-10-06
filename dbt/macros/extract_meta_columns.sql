{% macro extract_meta_columns(prefix = '') %}
    "isdeleted"::BOOLEAN AS {{prefix}}is_deleted,
    {{ to_timestamp("creationtime")}}::TIMESTAMP AS {{prefix}}creation_time,
    {{ to_timestamp("deletiontime") }}::TIMESTAMP AS {{prefix}}deletion_time,
    {{ to_timestamp("lastmodificationtime") }}::TIMESTAMP AS {{prefix}}last_modification_time
{% endmacro %}

