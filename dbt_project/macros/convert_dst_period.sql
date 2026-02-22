{% macro convert_dst_period(column_name) %}
    {#-
        Converts a DST period string to a DATE value.

        Supported formats (Snowflake dialect):
          Annual:    "2023"     → 2023-01-01
          Quarterly: "2023K3"   → 2023-07-01  (K1=Jan, K2=Apr, K3=Jul, K4=Oct)
          Monthly:   "2023M06"  → 2023-06-01
    -#}
    case
        when {{ column_name }} rlike '^[0-9]{4}K[1-4]$' then
            to_date(
                left({{ column_name }}, 4)
                || '-'
                || lpad(
                    ((cast(right({{ column_name }}, 1) as int) - 1) * 3 + 1)::varchar,
                    2, '0'
                   )
                || '-01',
                'YYYY-MM-DD'
            )
        when {{ column_name }} rlike '^[0-9]{4}M[0-9]{2}$' then
            to_date(
                left({{ column_name }}, 4)
                || '-'
                || substr({{ column_name }}, 6, 2)
                || '-01',
                'YYYY-MM-DD'
            )
        when {{ column_name }} rlike '^[0-9]{4}$' then
            to_date({{ column_name }} || '-01-01', 'YYYY-MM-DD')
        else null
    end
{% endmacro %}
