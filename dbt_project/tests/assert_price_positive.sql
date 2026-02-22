-- Singular test: assert that all housing prices are strictly positive.
-- Returns rows that violate the assertion; a non-empty result fails the test.

select
    housing_price_id,
    municipality_code,
    period_date,
    price_value
from {{ ref('fct_housing_transactions') }}
where price_value <= 0
