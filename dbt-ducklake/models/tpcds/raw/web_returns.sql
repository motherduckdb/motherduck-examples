from {{ source("tpc-ds", "web_returns") }}
{% if target.name == 'local' %} using sample 1 % {% endif %}
