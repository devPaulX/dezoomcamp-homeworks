{% macro safe_cast(column, data_type) %}
    {% if target.type == 'bigquery' %}
        SAFE_CAST({{ column }} AS {{ data_type }})
    {% else %}
        CAST({{ column }} AS {{ data_type }})
    {% endif %}
{% endmacro %}
