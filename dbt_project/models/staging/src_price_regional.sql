{{
    config(
        materialized='view'
    )
}}

with source as (

    select
        REGION          as region_name,
        EJENDOMSKATE    as ejendomskate,
        "BNØGLE"        as bnogle,
        TID             as tid,
        INDHOLD         as indhold,
        _LOADED_AT      as loaded_at
    from {{ source('raw', 'RAW_PRICE_REGIONAL') }}

),

-- Filter to one-family houses only (owner-occupied flats are heavily suppressed
-- outside Region Hovedstaden and would produce mostly NULL rows).
one_family as (

    select * from source
    where ejendomskate = 'One-family houses'

),

-- Pivot the BNØGLE metric-type dimension into columns.
-- Grain after pivot: one row per (region × property_category × month).
pivoted as (

    select
        region_name,
        ejendomskate                                        as property_category,
        tid                                                 as period_month,
        cast(left(tid, 4) as integer)                       as period_year,
        cast(right(tid, 2) as integer)                      as period_month_num,

        max(case when bnogle = 'Average price per property (dkk 1000)'
            then try_cast(indhold as float) end)            as avg_price_dkk_1000,

        max(case when bnogle = 'Sales in the price calculation (number)'
            then try_cast(indhold as float) end)            as sales_count,

        max(case when bnogle = 'Purchase sum in percent of taxable value'
            then try_cast(indhold as float) end)            as purchase_sum_pct_taxable,

        max(case when bnogle = 'Estimated number of sales'
            then try_cast(indhold as float) end)            as estimated_sales_count,

        max(loaded_at)                                      as _loaded_at

    from one_family
    group by
        region_name,
        ejendomskate,
        tid

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['region_name', 'property_category', 'period_month']) }}
                                    as price_regional_id,
        region_name,
        property_category,
        period_month,
        period_year,
        period_month_num,
        avg_price_dkk_1000,
        sales_count,
        purchase_sum_pct_taxable,
        estimated_sales_count,
        _loaded_at

    from pivoted

)

select * from final
