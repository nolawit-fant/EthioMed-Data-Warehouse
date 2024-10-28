-- macros/accepted_range.sql
{% macros accepted_range(model, column_name, min=0, max=13000) %}
SELECT *
FROM {{ model }}
WHERE {{ column_name }} < {{ min }} OR {{ column_name }} > {{ max }}
{% endmacro %}
