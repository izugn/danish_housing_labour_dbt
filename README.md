# Danish Housing & Labour Market Analytics

End-to-end ELT pipeline that ingests Statistics Denmark (DST) data into Snowflake,
transforms it with dbt Core, and orchestrates everything with Dagster.

Explores two research questions:
- Does rising unemployment predict slower housing price growth by region?
- Are regions with persistently low unemployment also the most expensive?

---

## Project Structure

```
danish_housing_labour/
├── ingestion/          # Python: DST StatBank API → Snowflake RAW schema
│   ├── src/
│   │   ├── tables.py   # DST table configs (variables, chunk sizes)
│   │   ├── fetch.py    # API fetch with retry, null sentinel handling
│   │   └── load.py     # Snowflake key-pair auth, write_pandas loader
│   └── run_ingestion.py
├── dbt_project/        # dbt Core: staging → core → marts
│   ├── models/
│   │   ├── staging/    # 5 source views, pivots, type casts
│   │   ├── core/       # dimension table + 2 incremental fact tables
│   │   └── marts/      # 2 analytics-ready views
│   ├── seeds/          # municipality → region mapping (98 municipalities)
│   └── docs/           # dbt docs overview page
└── orchestration/      # Dagster: daily refresh schedule
```

---

## Data Sources

| DST Table | Snowflake Raw Table | Description | Frequency |
|-----------|---------------------|-------------|-----------|
| EJ56 | RAW_PRICE_INDEX | Property price index (2022=100) by region | Quarterly |
| EJ131 | RAW_PRICE_REGIONAL | Sales key figures by region (avg price, volume) | Monthly |
| AUL01 | RAW_UNEMPLOYMENT | Gross unemployment by municipality | Annual |
| INDKP101 | RAW_LOCAL_INCOME | Disposable income by municipality | Annual |
| LABY22 | RAW_HOUSING_NATIONAL | National property sales benchmarks | Annual |

> **Note on geography:** Housing price data (EJ56, EJ131) is available at region level only.
> Municipality-level unemployment and income (AUL01, INDKP101) are aggregated to region via the
> `municipality_regions` seed. EJEN12 (municipality-level prices) was considered but found inactive since 2009.

---

## Key Models

| Model | Layer | Materialisation | Description |
|-------|-------|-----------------|-------------|
| `src_price_index` | Staging | View | EJ56 pivoted: TAL dimension → price_index, pct_change_qoq/yoy |
| `src_price_regional` | Staging | View | EJ131 pivoted: BNØGLE dimension → avg_price_dkk_1000, sales_count |
| `src_unemployment` | Staging | View | AUL01 filtered to gross unemployment, renamed columns |
| `src_local_income` | Staging | View | INDKP101 filtered to avg disposable income, renamed columns |
| `src_housing_national` | Staging | View | LABY22 pivoted: national price benchmarks |
| `dim_municipalities` | Core | Table | 98 municipalities → region mapping with English + Danish names |
| `fct_housing_prices` | Core | Incremental | Regional quarterly price index, one-family houses |
| `fct_labour_market_regional` | Core | Incremental | Regional annual unemployment + income aggregated from municipality |
| `mart_housing_affordability` | Marts | View | Price-to-income ratio per region per year |
| `mart_labour_housing_correlation` | Marts | View | YoY unemployment change vs price appreciation by region |

---

## Key Metrics

- `price_to_income_ratio` — average property price (DKK) divided by average disposable income (DKK) per region per year
- `avg_yoy_price_change_pct` — average year-over-year % price change from EJ56
- `total_gross_unemployment` — sum of gross unemployed across all municipalities in a region
- `avg_disposable_income_dkk` — population-weighted average disposable income per region per year

---

## Design Notes

**Region name bridge:** The seed uses English region names ("Capital Region of Denmark") while the DST API uses Danish names ("Region Hovedstaden"). `dim_municipalities` carries both via a `region_name_dst` column. `fct_housing_prices` joins on `region_name_dst` and exposes `region_name` (English) for consistency with the labour market fact table.

**DST null sentinels:** The DST API returns `..` for suppressed values and `:` for unavailable values. These are replaced with `NULL` during ingestion in `fetch.py` before loading to Snowflake.

**Incremental strategy:** Both core fact tables use `unique_key` + `_loaded_at` watermark for incremental loads. Run `dbt run --full-refresh` to rebuild from scratch.
