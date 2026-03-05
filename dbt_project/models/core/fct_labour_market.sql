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

earnings as (
    select * from {{ ref('src_local_income') }}
),

municipalities as (
    select * from {{ ref('dim_municipalities') }}
),

combined as (
    select
        {{ dbt_utils.generate_surrogate_key(['u.unemployment_id', 'e.earnings_id']) }} as labour_id,
        u.municipality_code,
        m.municipality_name,
        m.region_name,
        u.period_date,
        u.unemployment_count,
        e.industry_code,
        e.avg_monthly_earnings_dkk,
        greatest(u._loaded_at, e._loaded_at) as _loaded_at
    from unemployment u
    left join earnings e
        on  u.period_date = e.period_date
    left join municipalities m
        on  u.municipality_code = m.municipality_code

    {% if is_incremental() %}
    where greatest(u._loaded_at, e._loaded_at) > (select max(_loaded_at) from {{ this }})
    {% endif %}
)

select * from combined
