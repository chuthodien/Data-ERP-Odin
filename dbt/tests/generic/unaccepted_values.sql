{% test unaccepted_values(model, column_name, vals) %}

    select *
    from {{ model }}
    where {{ column_name }} is not in {{vals}}

{% endtest %}