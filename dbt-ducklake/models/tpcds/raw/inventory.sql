from {{ source("tpc-ds", "inventory") }}
{% if target.name == 'local' %} using sample 1 % {% endif %}