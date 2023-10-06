{%macro ms_to_day(time_col)%}
(COALESCE(
        {{time_col}}::FLOAT,
        0
    ) / 60 / 60 / 1000/8)
{%endmacro%}