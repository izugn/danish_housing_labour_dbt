{{
    config(
        materialized='view'
    )
}}

with source as (

    select
        "OMRÅDE"        as omraade,
        EJENDOMSKATE    as ejendomskate,
        TAL             as tal,
        TID             as tid,
        INDHOLD         as indhold,
        _LOADED_AT      as loaded_at
    from {{ source('raw', 'RAW_PRICE_INDEX') }}

),

-- Filter to 5 named regions + "All Denmark" only.
-- Province rows (landsdele) are excluded to keep the grain clean.
-- Known region values: "All Denmark", "Region Hovedstaden", "Region Midtjylland",
-- "Region Nordjylland", "Region Sjælland", "Region Syddanmark".
regions_only as (

    select * from source
    where omraade in (
        'All Denmark',
        'Region Hovedstaden',
        'Region Midtjylland',
        'Region Nordjylland',
        'Region Sjælland',
        'Region Syddanmark'
    )

),

-- Pivot the TAL metric-type dimension into columns.
-- Each source row is one (region × category × metric × quarter) combination.
-- After pivoting, each row is one (region × category × quarter).
pivoted as (

    select
        omraade                                     as region_name,
        ejendomskate                                as property_category,
        tid                                         as period_quarter,
        cast(left(tid, 4) as integer)               as period_year,
        cast(right(tid, 1) as integer)              as period_quarter_num,

        max(case when tal = 'Index'
            then try_cast(indhold as float) end)    as price_index,

        max(case when tal = 'Percentage change compared to previous quarter'
            then try_cast(indhold as float) end)    as pct_change_qoq,

        max(case when tal = 'Percentage change compared to same quarter the year before'
            then try_cast(indhold as float) end)    as pct_change_yoy,

        max(loaded_at)                              as _loaded_at

    from regions_only
    group by
        omraade,
        ejendomskate,
        tid

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['region_name', 'property_category', 'period_quarter']) }}
                                    as price_index_id,
        region_name,
        property_category,
        period_quarter,
        period_year,
        period_quarter_num,
        price_index,
        pct_change_qoq,
        pct_change_yoy,
        _loaded_at

    from pivoted

)

select * from final
