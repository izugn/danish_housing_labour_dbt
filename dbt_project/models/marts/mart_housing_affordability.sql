with housing as (
    select * from {{ ref('fct_housing_transactions') }}
),

labour as (
    select * from {{ ref('fct_labour_market') }}
),

affordability as (
    select
        h.period_date,
        h.municipality_code,
        h.municipality_name,
        h.region_name,
        h.price_value                                                   as avg_house_price_dkk,
        l.avg_monthly_earnings_dkk,
        l.unemployment_count,

        -- house price as a multiple of annual gross earnings
        case
            when l.avg_monthly_earnings_dkk > 0
            then round(h.price_value / (l.avg_monthly_earnings_dkk * 12), 2)
        end                                                             as price_to_income_ratio,

        -- year-over-year absolute price change
        h.price_value - lag(h.price_value) over (
            partition by h.municipality_code
            order by h.period_date
        )                                                               as yoy_price_change_dkk

    from housing h
    left join labour l
        on  h.municipality_code = l.municipality_code
        and h.period_date       = l.period_date
)

select * from affordability
