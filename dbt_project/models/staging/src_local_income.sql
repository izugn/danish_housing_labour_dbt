{{
    config(
        materialized='view'
    )
}}

with source as (

    select * from {{ source('raw', 'RAW_LOCAL_INCOME') }}

),

-- Filter to the single most useful metric:
-- Average disposable income per person in DKK, at municipality level.
-- ENHED = "Average income for all people (DKK)" gives the per-person average.
-- INDKOMSTTYPE = "1 Disposable income (2+30-31-32-35)" is the standard net income measure.
-- This produces one row per (municipality × year).
disposable_avg as (

    select * from source
    where
        ENHED         = 'Average income for all people (DKK)'
        and INDKOMSTTYPE = '1 Disposable income (2+30-31-32-35)'

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['OMRÅDE', 'TID']) }}
                                            as income_id,
        OMRÅDE                              as municipality_name,
        cast(TID as integer)                as period_year,
        try_cast(INDHOLD as float)          as avg_disposable_income_dkk,
        _LOADED_AT                          as _loaded_at

    from disposable_avg

),

final as (

    select * from renamed
    where avg_disposable_income_dkk is not null

)

select * from final
