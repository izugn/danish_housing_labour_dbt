{{
    config(materialized='table')
}}

with seed_regions as (
    select * from {{ ref('municipality_regions') }}
),

final as (
    select
        municipality_code,
        municipality_name,
        region_code,
        region_name,
        current_timestamp as _loaded_at
    from seed_regions
)

select * from final
