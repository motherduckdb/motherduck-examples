from {{ source("tpc-ds", "catalog_sales") }}
{% if var('sample') %} using sample 1 % {% endif %}
