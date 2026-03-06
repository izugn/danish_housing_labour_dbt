{{
    config(
        materialized='incremental',
        unique_key='labour_regional_id',
        on_schema_change='sync_all_columns'
    )
}}

with unemployment as (

    select * from {{ ref('src_unemployment') }}

    {% if is_incremental() %}
        and _loaded_at > (select max(_loaded_at) from {{ this }})
    {% endif %}

),

income as (

    select * from {{ ref('src_local_income') }}

    {% if is_incremental() %}
        and _loaded_at > (select max(_loaded_at) from {{ this }})
    {% endif %}

),

municipalities as (

    select * from {{ ref('dim_municipalities') }}

),

-- Aggregate unemployment to region level via municipality join.
-- Inner join intentionally excludes area_name values that are regions
-- or "All Denmark" (they won't match municipality_name in the seed).
unemployment_regional as (

    select
        m.region_name,
        u.period_year,
        sum(u.gross_unemployment_count)     as total_gross_unemployment,
        count(distinct u.area_name)         as municipality_count_unemp

    from unemployment u
    inner join municipalities m
        on u.area_name = m.municipality_name
    group by
        m.region_name,
        u.period_year

),

-- Aggregate income to region level via municipality join.
income_regional as (

    select
        m.region_name,
        i.period_year,
        avg(i.avg_disposable_income_dkk)    as avg_disposable_income_dkk,
        count(distinct i.municipality_name) as municipality_count_income

    from income i
    inner join municipalities m
        on i.municipality_name = m.municipality_name
    group by
        m.region_name,
        i.period_year

),

joined as (

    select
        coalesce(u.region_name, i.region_name)      as region_name,
        coalesce(u.period_year, i.period_year)       as period_year,
        u.total_gross_unemployment,
        i.avg_disposable_income_dkk,
        coalesce(
            u.municipality_count_unemp,
            i.municipality_count_income
        )                                           as municipality_count

    from unemployment_regional u
    full outer join income_regional i
        on  u.region_name = i.region_name
        and u.period_year = i.period_year

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['region_name', 'period_year']) }}
                                        as labour_regional_id,
        region_name,
        period_year,
        total_gross_unemployment,
        avg_disposable_income_dkk,
        municipality_count

    from joined
    where region_name is not null

)

select * from final