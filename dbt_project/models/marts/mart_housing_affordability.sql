{{
    config(materialized='view')
}}

with housing as (

    select
        region_name,
        region_name_dst,
        period_year,

        -- Annual averages from quarterly data
        round(avg(price_index), 1)        as avg_annual_price_index,
        round(avg(pct_change_yoy), 2)     as avg_yoy_price_change_pct,
        count(*)                as quarters_with_data

    from {{ ref('fct_housing_prices') }}
    where price_index is not null
    group by
        region_name,
        region_name_dst,
        period_year

),

labour as (

    select
        region_name,
        period_year,
        total_gross_unemployment,
        avg_disposable_income_dkk,
        municipality_count

    from {{ ref('fct_labour_market_regional') }}

),

-- EJ131 absolute prices for price-to-income ratio
-- Aggregate monthly to annual average
regional_prices as (

    select
        region_name,
        period_year,
        -- Convert from DKK thousands to DKK millions for readability
        -- Source value e.g. 2455.1 (thousands) → 2.46 (millions)
        round(avg(avg_price_dkk_1000) / 1000, 2)   as avg_annual_price_mdkk,
        sum(sales_count)                            as annual_sales_count

    from {{ ref('src_price_regional') }}
    where avg_price_dkk_1000 is not null
    group by
        region_name,
        period_year

),

joined as (

    select
        h.region_name,
        h.region_name_dst,
        h.period_year,
        h.avg_annual_price_index,
        h.avg_yoy_price_change_pct,
        h.quarters_with_data,
        l.total_gross_unemployment,
        round(l.avg_disposable_income_dkk, 0)   as avg_disposable_income_dkk,
        l.municipality_count,
        rp.avg_annual_price_mdkk,
        rp.annual_sales_count,

        -- Price-to-income ratio: avg property price vs avg disposable income
        -- avg_annual_price_mdkk is in DKK millions → multiply by 1,000,000
        case
            when l.avg_disposable_income_dkk > 0
                and rp.avg_annual_price_mdkk is not null
            then round(
                (rp.avg_annual_price_mdkk * 1000000) / l.avg_disposable_income_dkk,
                2
            )
        end                                         as price_to_income_ratio

    from housing h
    left join labour l
        on  h.region_name  = l.region_name
        and h.period_year  = l.period_year
    left join regional_prices rp
        -- EJ131 uses Danish region names; join on region_name_dst
        on  h.region_name_dst = rp.region_name
        and h.period_year     = rp.period_year

)

select * from joined
