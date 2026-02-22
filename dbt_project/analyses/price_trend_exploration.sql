-- Ad-hoc exploration: housing price trends by region over time.
-- Run with:  dbt compile --select price_trend_exploration
-- Inspect the rendered SQL at:  target/compiled/.../price_trend_exploration.sql

with trends as (
    select
        period_date,
        region_name,
        round(avg(avg_house_price_dkk), 0)     as avg_price_dkk,
        round(avg(price_to_income_ratio), 2)   as avg_price_to_income_ratio,
        count(distinct municipality_code)      as n_municipalities
    from {{ ref('mart_housing_affordability') }}
    group by 1, 2
),

ranked as (
    select
        *,
        rank() over (partition by period_date order by avg_price_dkk desc) as price_rank
    from trends
)

select *
from ranked
order by period_date, price_rank
