{{
    config(
        materialized='view'
    )
}}

with source as (

    select * from {{ source('raw', 'RAW_PRICE_INDEX') }}

),

-- Filter to 5 named regions + "All Denmark" only.
-- Province rows (landsdele) are excluded to keep the grain clean.
-- Known region values: "All Denmark", "Region Hovedstaden", "Region Midtjylland",
-- "Region Nordjylland", "Region Sjælland", "Region Syddanmark".
regions_only as (

    select * from source
    where OMRÅDE in (
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
        OMRÅDE                                    as region_name,
        EJENDOMSKATE                              as property_category,
        TID                                       as period_quarter,

        -- Extract integer year from period string like "2024Q1" → 2024
        cast(left(TID, 4) as integer)             as period_year,

        -- Extract quarter number: "2024Q1" → 1
        cast(right(TID, 1) as integer)            as period_quarter_num,

        max(case when TAL = 'Index'
            then try_cast(INDHOLD as float) end)  as price_index,

        max(case when TAL = 'Percentage change compared to previous quarter'
            then try_cast(INDHOLD as float) end)  as pct_change_qoq,

        max(case when TAL = 'Percentage change compared to same quarter the year before'
            then try_cast(INDHOLD as float) end)  as pct_change_yoy,

        max(_LOADED_AT)                           as _loaded_at

    from regions_only
    group by
        OMRÅDE,
        EJENDOMSKATE,
        TID

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
