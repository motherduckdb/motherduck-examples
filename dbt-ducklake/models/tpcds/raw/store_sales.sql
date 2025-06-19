from {{ source("tpc-ds", "store_sales") }}
{% if target.name == 'local' %} using sample 1 % {% endif %}
