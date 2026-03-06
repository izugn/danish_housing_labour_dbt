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
