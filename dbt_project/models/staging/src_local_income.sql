{{
    config(
        materialized='view'
    )
}}

with source as (

    select
        "OMRÅDE"        as omraade,
        ENHED           as enhed,
        INDKOMSTTYPE    as indkomsttype,
        TID             as tid,
        INDHOLD         as indhold,
        _LOADED_AT      as loaded_at
    from {{ source('raw', 'RAW_LOCAL_INCOME') }}

),

-- Filter to the single most useful metric:
-- Average disposable income per person in DKK, at municipality level.
-- ENHED = "Average income for all people (DKK)" gives the per-person average.
-- INDKOMSTTYPE = "1 Disposable income (2+30-31-32-35)" is the standard net income measure.
-- This produces one row per (municipality × year).

-- NOTE: Uses arithmetic mean disposable income (INDKP101).
-- Mean is sensitive to high-income outliers and will overstate
-- typical purchasing power in municipalities with high income inequality
-- (e.g. Gentofte, Rudersdal). Median would be preferable but is not
-- available at municipality level in the DST StatBank API.
-- Price-to-income ratios should be interpreted as a lower-bound estimate
-- of unaffordability.


disposable_avg as (

    select * from source
    where
        enhed         = 'Average income for all people (DKK)'
        and indkomsttype = '1 Disposable income (2+30-31-32-35)'

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['omraade', 'tid']) }}
                                            as income_id,
        omraade                             as municipality_name,
        cast(tid as integer)                as period_year,
        try_cast(indhold as float)          as avg_disposable_income_dkk,
        loaded_at                           as _loaded_at

    from disposable_avg

),

final as (

    select * from renamed
    where avg_disposable_income_dkk is not null

)

select * from final
