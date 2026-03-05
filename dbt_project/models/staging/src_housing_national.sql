{{
    config(
        materialized='view'
    )
}}

with source as (

    select
        EJENDOMSKATE    as ejendomskate,
        "BNØGLE"        as bnogle,
        TID             as tid,
        INDHOLD         as indhold,
        _LOADED_AT      as loaded_at
    from {{ source('raw', 'RAW_HOUSING_NATIONAL') }}

),

-- Filter to one-family houses as the primary property type.
one_family as (

    select * from source
    where ejendomskate = 'One-family houses'

),

-- Pivot the BNØGLE metric-type dimension into columns.
-- Grain after pivot: one row per (property_category × year).
pivoted as (

    select
        ejendomskate                                            as property_category,
        cast(tid as integer)                                    as period_year,

        max(case when bnogle = 'Average price per property (DKK 1,000)'
            then try_cast(indhold as float) end)                as avg_price_per_property_dkk_1000,

        max(case when bnogle = 'Average price per square meter per property (DKK per m2)'
            then try_cast(indhold as float) end)                as avg_price_per_m2_dkk,

        max(case when bnogle = 'Average age at which all buyers are first-time buyers (age)'
            then try_cast(indhold as float) end)                as first_time_buyer_avg_age,

        max(case when bnogle = 'Estimated number of sales'
            then try_cast(indhold as float) end)                as estimated_sales_count,

        max(loaded_at)                                          as _loaded_at

    from one_family
    group by
        ejendomskate,
        tid

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
