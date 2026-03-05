{{
    config(
        materialized='view'
    )
}}

with source as (

    select * from {{ source('raw', 'RAW_PRICE_REGIONAL') }}

),

-- Filter to one-family houses only (owner-occupied flats are heavily suppressed
-- outside Region Hovedstaden and would produce mostly NULL rows).
one_family as (

    select * from source
    where EJENDOMSKATE = 'One-family houses'

),

-- Pivot the BNØGLE metric-type dimension into columns.
-- Grain after pivot: one row per (region × property_category × month).
pivoted as (

    select
        REGION                                          as region_name,
        EJENDOMSKATE                                    as property_category,
        TID                                             as period_month,

        -- Extract integer year from period string like "2006M01" → 2006
        cast(left(TID, 4) as integer)                   as period_year,

        -- Extract integer month: "2006M01" → 1
        cast(right(TID, 2) as integer)                  as period_month_num,

        max(case when BNØGLE = 'Average price per property (dkk 1000)'
            then try_cast(INDHOLD as float) end)        as avg_price_dkk_1000,

        max(case when BNØGLE = 'Sales in the price calculation (number)'
            then try_cast(INDHOLD as float) end)        as sales_count,

        max(case when BNØGLE = 'Purchase sum in percent of taxable value'
            then try_cast(INDHOLD as float) end)        as purchase_sum_pct_taxable,

        max(case when BNØGLE = 'Estimated number of sales'
            then try_cast(INDHOLD as float) end)        as estimated_sales_count,

        max(_LOADED_AT)                                 as _loaded_at

    from one_family
    group by
        REGION,
        EJENDOMSKATE,
        TID

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
