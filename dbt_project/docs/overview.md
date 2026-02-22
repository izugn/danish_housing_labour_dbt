{% docs __overview__ %}

# Danish Housing & Labour Market Analytics

This dbt project transforms raw Statistics Denmark (DST) data into analytics-ready
models for exploring the relationship between housing prices and labour market
conditions across Danish municipalities.

---

## Data Sources

| Snowflake Table      | DST Table | Description                          |
|----------------------|-----------|--------------------------------------|
| `RAW_HOUSING_PRICES` | EJEN12    | Property sale prices per m²          |
| `RAW_UNEMPLOYMENT`   | AUL01     | Registered unemployment by area      |
| `RAW_EARNINGS`       | LONS10    | Average monthly earnings by industry |

---

## Model Layers

| Layer       | Folder      | Materialisation | Purpose                                         |
|-------------|-------------|------------------|-------------------------------------------------|
| **Staging** | `staging/`  | View             | Rename & cast raw columns, basic freshness tests |
| **Core**    | `core/`     | Incremental      | Fact tables and municipality dimension           |
| **Marts**   | `marts/`    | View             | Analytics-ready, join-ready outputs              |

---

## Key Metrics

- **`price_to_income_ratio`** — house price as a multiple of annual gross earnings
- **`affordability_tier`** — AFFORDABLE / MODERATE_UNAFFORDABILITY / HIGH_UNAFFORDABILITY
- **`yoy_price_change_dkk`** — year-over-year absolute price change per municipality

---

## Refresh Schedule

Models are refreshed daily at 06:00 UTC via Dagster.
See `orchestration/` for pipeline and schedule definitions.

{% enddocs %}
