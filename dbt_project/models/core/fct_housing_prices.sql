{{
    config(
        materialized='incremental',
        unique_key='price_index_id',
        on_schema_change='sync_all_columns'
    )
}}

with src as (

    select * from {{ ref('src_price_index') }}
    where property_category = 'One-family houses'

    {% if is_incremental() %}
        and _loaded_at > (select max(_loaded_at) from {{ this }})
    {% endif %}

),

-- Get the region name translation from dim_municipalities.
-- We only need one row per DST region name, so use distinct.
region_map as (

    select distinct
        region_name_dst,
        region_name

    from {{ ref('dim_municipalities') }}
    where region_name_dst is not null

),

-- Enrich with the English region name for consistency with fct_labour_market_regional
enriched as (

    select
        src.price_index_id,
        src.region_name         as region_name_dst,
        r.region_name           as region_name,
        src.property_category,
        src.period_quarter,
        src.period_year,
        src.period_quarter_num,
        src.price_index,
        src.pct_change_qoq,
        src.pct_change_yoy,
        src._loaded_at

    from src
    left join region_map r
        on src.region_name = r.region_name_dst

),

final as (

    -- Exclude "All Denmark" row — it has no matching region in the seed
    -- and is better handled as a national benchmark in the marts layer
    select * from enriched
    where region_name_dst != 'All Denmark'

)

select * from final