from {{ source("tpc-ds", "catalog_returns") }}
{% if target.name == 'local' %} using sample 1 % {% endif %}
