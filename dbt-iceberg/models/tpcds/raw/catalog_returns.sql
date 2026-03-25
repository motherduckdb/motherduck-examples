from {{ source("tpc-ds", "catalog_returns") }}
{% if var('sample') %} using sample 1 % {% endif %}
