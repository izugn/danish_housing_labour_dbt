{{
    config(
        materialized='incremental',
        unique_key='housing_price_id',
        incremental_strategy='merge',
        on_schema_change='sync_all_columns'
    )
}}

with housing as (
    select * from {{ ref('src_price_regional') }}
),

municipalities as (
    select * from {{ ref('dim_municipalities') }}
),

final as (
    select
        h.housing_price_id,
        h.municipality_code,
        m.municipality_name,
        m.region_name,
        h.unit_type,
        h.period_date,
        h.raw_period,
        h.price_value,
        h._loaded_at
    from housing h
    left join municipalities m
        on h.municipality_code = m.municipality_code

    {% if is_incremental() %}
    where h._loaded_at > (select max(_loaded_at) from {{ this }})
    {% endif %}
)

select * from final
