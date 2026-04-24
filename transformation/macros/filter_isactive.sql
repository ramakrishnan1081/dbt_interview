{% macro filter_isactive(column_name='active__c') %}
    {{ column_name }} = 'Yes'
{% endmacro %}