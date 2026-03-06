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
