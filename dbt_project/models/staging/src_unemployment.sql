with source as (
    select * from {{ source('raw', 'RAW_UNEMPLOYMENT') }}
),

renamed as (
    select
        -- identifiers
        {{ dbt_utils.generate_surrogate_key(['OMRÅDE', 'TID']) }} as unemployment_id,
        cast(OMRÅDE  as varchar)  as municipality_code,

        -- period
        {{ convert_dst_period('TID') }}  as period_date,
        cast(TID     as varchar)         as raw_period,

        -- metrics
        cast(replace(INDHOLD, '.', '') as numeric(10, 2)) as unemployment_count,

        -- metadata
        current_timestamp as _loaded_at

    from source
    where INDHOLD is not null
)

select * from renamed
