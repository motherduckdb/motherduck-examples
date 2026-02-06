from {{ source("tpc-ds", "inventory") }}
{% if var('sample') %} using sample 1 % {% endif %}