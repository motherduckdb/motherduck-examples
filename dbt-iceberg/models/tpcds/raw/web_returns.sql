from {{ source("tpc-ds", "web_returns") }}
{% if var('sample') %} using sample 1 % {% endif %}
