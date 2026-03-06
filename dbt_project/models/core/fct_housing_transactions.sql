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

-- Translate DST Danish region names to the English convention used in the seed /
-- dim_municipalities.  Any region that does not match (e.g. national aggregates) is
-- intentionally excluded by the subsequent INNER JOIN.
dst_to_en as (

    select
        region_name                                 as region_name_dst,
        case region_name
            when 'Region Hovedstaden' then 'Capital Region of Denmark'
            when 'Region Sjælland'    then 'Region Zealand'
            when 'Region Syddanmark'  then 'Region of Southern Denmark'
            when 'Region Midtjylland' then 'Central Denmark Region'
            when 'Region Nordjylland' then 'North Denmark Region'
        end                                         as region_name,
        property_category,
        period_year,
        avg_price_dkk_1000,
        _loaded_at

    from housing

),

-- Expand regional housing prices to municipality grain.
-- Each municipality inherits the annual-average price of its region.
-- Grain after aggregation: one row per municipality × property_category × year.
annual_by_municipality as (

    select
        m.municipality_code,
        m.municipality_name,
        m.region_code,
        m.region_name,
        d.region_name_dst,
        d.property_category,
        d.period_year,
        date_from_parts(d.period_year, 1, 1)            as period_date,
        round(avg(d.avg_price_dkk_1000) * 1000, 0)     as price_value,
        max(d._loaded_at)                               as _loaded_at

    from dst_to_en d
    inner join municipalities m
        on d.region_name = m.region_name

    group by 1, 2, 3, 4, 5, 6, 7, 8

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['municipality_code', 'property_category', 'period_year']) }}
                                as housing_price_id,
        municipality_code,
        municipality_name,
        region_code,
        region_name,
        region_name_dst,
        property_category,
        period_year,
        period_date,
        price_value,
        _loaded_at

    from annual_by_municipality

    {% if is_incremental() %}
    where _loaded_at > (select max(_loaded_at) from {{ this }})
    {% endif %}

)

select * from final
