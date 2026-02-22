{% snapshot scd_municipalities %}

{{
    config(
        target_schema='snapshots',
        unique_key='municipality_code',
        strategy='check',
        check_cols=['municipality_name', 'region_code', 'region_name'],
        invalidate_hard_deletes=true,
    )
}}

select
    municipality_code,
    municipality_name,
    region_code,
    region_name,
    current_timestamp as updated_at
from {{ ref('dim_municipalities') }}

{% endsnapshot %}
