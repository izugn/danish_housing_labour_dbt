{{
    config(
        materialized='view'
    )
}}

with source as (

    select
        YDELSESTYPE     as ydelsestype,
        "OMRÅDE"        as omraade,
        TID             as tid,
        INDHOLD         as indhold,
        _LOADED_AT      as loaded_at
    from {{ source('raw', 'RAW_UNEMPLOYMENT') }}

),

-- Filter to gross unemployment only (the most comparable cross-area metric).
-- Retains all area types: municipalities, regions, and "All Denmark".
gross_only as (

    select * from source
    where ydelsestype = 'Gross unemployment'

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['omraade', 'tid']) }}
                                        as unemployment_id,
        omraade                         as area_name,
        cast(tid as integer)            as period_year,
        try_cast(indhold::varchar as float) as gross_unemployment_count,
        loaded_at                       as _loaded_at

    from gross_only

),

final as (

    select * from renamed
    where gross_unemployment_count is not null

)

select * from final
