{% macro extract_province_code(
        province_col
    ) %}
    (
        SELECT
            subdivision_code
        FROM
            {{ ref('province_codes') }}
            pc
        WHERE
            ({{ province_col }} = pc.subdivision_name)
            OR (
                (pc.subdivision_matcher IS NOT NULL)
                AND
                ({{ province_col }} like pc.subdivision_matcher)
            )
    )
{% endmacro %}
