{% macro maintain_ducklake() %}
    {% set log_prefix = "🦆 Ducklake Maintenance" %}
    
    {# Get the ducklake catalog alias from profiles.yml attach configuration #}
    {% set ns = namespace(ducklake_alias=none) %}
    
    {# Try to fetch from target.attach if available #}
    {% if target.attach is defined and target.attach %}
        {% for attachment in target.attach %}
            {% if 'ducklake' in attachment.path %}
                {% set ns.ducklake_alias = attachment.alias %}
            {% endif %}
        {% endfor %}
    {% endif %}
    
    {# Fallback to 'catalog' if not found #}
    {% if ns.ducklake_alias is none %}
        {% set ns.ducklake_alias = 'catalog' %}
        {{ log(log_prefix ~ " | Warning: Could not find ducklake alias in target.attach, using default 'catalog'", info=True) }}
    {% endif %}
    
    {% set ducklake_alias = ns.ducklake_alias %}
    
    {{ log(log_prefix ~ " | Starting maintenance operations on '" ~ ducklake_alias ~ "'...", info=True) }}
    
    {# Merge adjacent files #}
    {{ log(log_prefix ~ " | Merging adjacent files...", info=True) }}
    {% set merge_query %}
        CALL {{ ducklake_alias }}.merge_adjacent_files();
    {% endset %}
    {% do run_query(merge_query) %}
    {{ log(log_prefix ~ " | ✓ Merged adjacent files", info=True) }}
    
    {# Expire old snapshots #}
    {{ log(log_prefix ~ " | Expiring snapshots older than 1 hour...", info=True) }}
    {% set expire_query %}
        CALL ducklake_expire_snapshots('{{ ducklake_alias }}', older_than => now() - INTERVAL '1 hour');
    {% endset %}
    {% do run_query(expire_query) %}
    {{ log(log_prefix ~ " | ✓ Expired old snapshots", info=True) }}
    
    {# Cleanup old files #}
    {{ log(log_prefix ~ " | Cleaning up old files...", info=True) }}
    {% set cleanup_query %}
        CALL ducklake_cleanup_old_files('{{ ducklake_alias }}', cleanup_all => true);
    {% endset %}
    {% do run_query(cleanup_query) %}
    {{ log(log_prefix ~ " | ✓ Cleaned up old files", info=True) }}
    
    {{ log(log_prefix ~ " | All maintenance operations completed successfully! 🎉", info=True) }}
{% endmacro %}

