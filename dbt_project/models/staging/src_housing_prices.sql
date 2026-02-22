with source as (
    select * from {{ source('raw', 'RAW_HOUSING_PRICES') }}
),

renamed as (
    select
        -- identifiers
        {{ dbt_utils.generate_surrogate_key(['OMRÅDE', 'ENHED', 'TID']) }} as housing_price_id,
        cast(OMRÅDE  as varchar)  as municipality_code,
        cast(ENHED   as varchar)  as unit_type,

        -- period
        {{ convert_dst_period('TID') }}  as period_date,
        cast(TID     as varchar)         as raw_period,

        -- metrics
        cast(replace(INDHOLD, '.', '') as numeric(18, 2)) as price_value,

        -- metadata
        current_timestamp as _loaded_at

    from source
    where INDHOLD is not null
)

select * from renamed
