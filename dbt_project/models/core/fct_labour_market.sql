{{
    config(
        materialized='incremental',
        unique_key='labour_id',
        incremental_strategy='merge',
        on_schema_change='sync_all_columns'
    )
}}

with unemployment as (
    select * from {{ ref('src_unemployment') }}
),

income as (
    select * from {{ ref('src_local_income') }}
),

municipalities as (
    select * from {{ ref('dim_municipalities') }}
),

-- Join unemployment to dim_municipalities on area_name = municipality_name.
-- This filters area_name rows to municipality-level only (regions and "All Denmark"
-- do not appear in dim_municipalities and are automatically excluded).
combined as (

    select
        {{ dbt_utils.generate_surrogate_key(['m.municipality_code', 'u.period_year']) }}
                                                    as labour_id,
        m.municipality_code,
        m.municipality_name,
        m.region_name,
        u.period_year,
        date_from_parts(u.period_year, 1, 1)        as period_date,
        u.gross_unemployment_count                  as unemployment_count,
        i.avg_disposable_income_dkk,
        round(i.avg_disposable_income_dkk / 12, 0) as avg_monthly_earnings_dkk,
        greatest(
            u._loaded_at,
            coalesce(i._loaded_at, u._loaded_at)
        )                                           as _loaded_at

    from unemployment u
    inner join municipalities m
        on u.area_name = m.municipality_name
    left join income i
        on  m.municipality_name = i.municipality_name
        and u.period_year       = i.period_year

    {% if is_incremental() %}
    where greatest(u._loaded_at, coalesce(i._loaded_at, u._loaded_at))
          > (select max(_loaded_at) from {{ this }})
    {% endif %}

)

select * from combined
