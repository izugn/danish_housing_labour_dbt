with base as (
    select * from {{ ref('mart_housing_affordability') }}
),

with_affordability_tier as (
    select
        region_name,
        region_name_dst,
        period_year,
        avg_annual_price_index,
        avg_yoy_price_change_pct,
        quarters_with_data,
        total_gross_unemployment,
        avg_disposable_income_dkk,
        municipality_count,
        avg_annual_price_dkk_1000,
        annual_sales_count,
        price_to_income_ratio,
        case
            when price_to_income_ratio > 10 then 'HIGH_UNAFFORDABILITY'
            when price_to_income_ratio > 7  then 'MODERATE_UNAFFORDABILITY'
            else 'AFFORDABLE'
        end as affordability_tier
    from base
    where region_name is not null
)

select * from with_affordability_tier
