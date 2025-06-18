from {{ source("tpc-ds", "store_returns") }}
{% if var("sample", false) %} using sample 1 % {% endif %}
