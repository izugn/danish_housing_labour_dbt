{{
    config(
        materialized='view'
    )
}}

with source as (

    select * from {{ source('raw', 'RAW_HOUSING_NATIONAL') }}

),

-- Filter to one-family houses as the primary property type.
one_family as (

    select * from source
    where EJENDOMSKATE = 'One-family houses'

),

-- Pivot the BNØGLE metric-type dimension into columns.
-- Grain after pivot: one row per (property_category × year).
pivoted as (

    select
        EJENDOMSKATE                                        as property_category,
        cast(TID as integer)                                as period_year,

        max(case when BNØGLE = 'Average price per property (DKK 1,000)'
            then try_cast(INDHOLD as float) end)            as avg_price_per_property_dkk_1000,

        max(case when BNØGLE = 'Average price per square meter per property (DKK per m2)'
            then try_cast(INDHOLD as float) end)            as avg_price_per_m2_dkk,

        max(case when BNØGLE = 'Average age at which all buyers are first-time buyers (age)'
            then try_cast(INDHOLD as float) end)            as first_time_buyer_avg_age,

        max(case when BNØGLE = 'Estimated number of sales'
            then try_cast(INDHOLD as float) end)            as estimated_sales_count,

        max(_LOADED_AT)                                     as _loaded_at

    from one_family
    group by
        EJENDOMSKATE,
        TID

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['property_category', 'period_year']) }}
                                    as housing_national_id,
        property_category,
        period_year,
        avg_price_per_property_dkk_1000,
        avg_price_per_m2_dkk,
        first_time_buyer_avg_age,
        estimated_sales_count,
        _loaded_at

    from pivoted

)

select * from final
