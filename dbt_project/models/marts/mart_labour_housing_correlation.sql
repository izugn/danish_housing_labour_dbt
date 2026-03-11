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
        avg_annual_price_mdkk,
        annual_sales_count,
        price_to_income_ratio,
        case
            when price_to_income_ratio is null  then null
            when price_to_income_ratio > 10 then 'HIGH_UNAFFORDABILITY'
            when price_to_income_ratio > 7  then 'MODERATE_UNAFFORDABILITY'
            else 'AFFORDABLE'
        end as affordability_tier,
        case
            when total_gross_unemployment is not null
             and avg_annual_price_mdkk    is not null
             and avg_disposable_income_dkk is not null
             and price_to_income_ratio    is not null
            then true
            else false
        end                                         as has_full_data,
        -- True only when the year has all 4 quarters of price index data.
        -- Filters out partial-year rows (e.g. 2025 with only 3 quarters).
        case
            when quarters_with_data = 4 then true
            else false
        end                                     as is_full_year
    from base
    where region_name is not null
)

select * from with_affordability_tier
