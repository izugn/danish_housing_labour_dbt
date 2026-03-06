{{
    config(materialized='view')
}}

with housing as (

    select
        region_name,
        region_name_dst,
        period_year,

        -- Annual averages from quarterly data
        avg(price_index)        as avg_annual_price_index,
        avg(pct_change_yoy)     as avg_yoy_price_change_pct,
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
        avg(avg_price_dkk_1000)     as avg_annual_price_dkk_1000,
        sum(sales_count)            as annual_sales_count

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
        l.avg_disposable_income_dkk,
        l.municipality_count,
        rp.avg_annual_price_dkk_1000,
        rp.annual_sales_count,

        -- Price-to-income ratio: average property price vs average disposable income
        -- avg_annual_price_dkk_1000 is in DKK thousands → multiply by 1000
        case
            when l.avg_disposable_income_dkk > 0
                and rp.avg_annual_price_dkk_1000 is not null
            then round(
                (rp.avg_annual_price_dkk_1000 * 1000) / l.avg_disposable_income_dkk,
                2
            )
        end                         as price_to_income_ratio

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
