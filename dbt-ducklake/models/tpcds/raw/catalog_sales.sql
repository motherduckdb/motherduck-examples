from {{ source("tpc-ds", "catalog_sales") }}
{% if target.name == 'local' %} using sample 1 % {% endif %}
