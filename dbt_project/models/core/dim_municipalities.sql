{{
    config(materialized='table')
}}

with seed_regions as (

    select * from {{ ref('municipality_regions') }}

),

-- Add the Danish DST API region name alongside the English name.
-- This translation is needed because housing price tables (EJ56, EJ131)
-- use Danish region names while the seed uses English names.
final as (

    select
        municipality_code,
        municipality_name,
        region_code,
        region_name,

        case region_name
            when 'Capital Region of Denmark' then 'Region Hovedstaden'
            when 'Region Zealand'            then 'Region Sjælland'
            when 'Region of Southern Denmark' then 'Region Syddanmark'
            when 'Central Denmark Region'    then 'Region Midtjylland'
            when 'North Denmark Region'      then 'Region Nordjylland'
        end                                 as region_name_dst,

        current_timestamp                   as _loaded_at

    from seed_regions

)

select * from final
