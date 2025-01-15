from {{ source("tpc-ds", "web_sales") }}
{% if target.name == 'local' %} using sample 1 % {% endif %}
