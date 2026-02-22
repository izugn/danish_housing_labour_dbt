with base as (
    select * from {{ ref('mart_housing_affordability') }}
),

regional_aggregates as (
    select
        period_date,
        region_name,
        round(avg(avg_house_price_dkk), 0)       as region_avg_house_price_dkk,
        round(avg(avg_monthly_earnings_dkk), 0)  as region_avg_monthly_earnings_dkk,
        sum(unemployment_count)                  as region_total_unemployed,
        round(avg(price_to_income_ratio), 2)     as region_avg_price_to_income_ratio,
        count(distinct municipality_code)        as municipality_count
    from base
    where region_name is not null
    group by 1, 2
),

with_affordability_tier as (
    select
        *,
        case
            when region_avg_price_to_income_ratio > 10 then 'HIGH_UNAFFORDABILITY'
            when region_avg_price_to_income_ratio > 7  then 'MODERATE_UNAFFORDABILITY'
            else 'AFFORDABLE'
        end as affordability_tier
    from regional_aggregates
)

select * from with_affordability_tier
