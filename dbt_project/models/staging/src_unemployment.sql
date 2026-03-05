{{
    config(
        materialized='view'
    )
}}

with source as (

    select * from {{ source('raw', 'RAW_UNEMPLOYMENT') }}

),

-- Filter to gross unemployment only (the most comparable cross-area metric).
-- Retains all area types: municipalities, regions, and "All Denmark".
gross_only as (

    select * from source
    where YDELSESTYPE = 'Gross unemployment'

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['OMRÅDE', 'TID']) }}
                                        as unemployment_id,
        OMRÅDE                          as area_name,
        cast(TID as integer)            as period_year,
        try_cast(INDHOLD as float)      as gross_unemployment_count,
        _LOADED_AT                      as _loaded_at

    from gross_only

),

final as (

    select * from renamed
    where gross_unemployment_count is not null

)

select * from final
