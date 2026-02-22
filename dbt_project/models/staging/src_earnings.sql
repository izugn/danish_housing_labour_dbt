with source as (
    select * from {{ source('raw', 'RAW_EARNINGS') }}
),

renamed as (
    select
        -- identifiers
        {{ dbt_utils.generate_surrogate_key(['BRANCHE', 'TID']) }} as earnings_id,
        cast(BRANCHE as varchar)  as industry_code,

        -- period
        {{ convert_dst_period('TID') }}  as period_date,
        cast(TID     as varchar)         as raw_period,

        -- metrics
        cast(replace(INDHOLD, '.', '') as numeric(12, 2)) as avg_monthly_earnings_dkk,

        -- metadata
        current_timestamp as _loaded_at

    from source
    where INDHOLD is not null
)

select * from renamed
