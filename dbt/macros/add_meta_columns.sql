{% macro add_meta_columns(prefix = '') %}
    "is_deleted"::BOOLEAN AS {{prefix}}is_deleted,
    "creation_time"AS {{prefix}}creation_time,
    "deletion_time"::TIMESTAMP AS {{prefix}}deletion_time,
    "last_modification_time"::TIMESTAMP AS {{prefix}}last_modification_time
{% endmacro %}

