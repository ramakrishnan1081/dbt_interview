{% macro filter_not_deleted(column_name='isdeleted') %}
    {{ column_name }} = 0
{% endmacro %}