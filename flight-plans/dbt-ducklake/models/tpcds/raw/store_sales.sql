from {{ source("tpc-ds", "store_sales") }}
{% if var('sample') %} using sample 1 % {% endif %}
