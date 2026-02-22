# Danish Housing & Labour Market Analytics

End-to-end pipeline that ingests Statistics Denmark (DST) data into Snowflake,
transforms it with dbt Core, and orchestrates everything with Dagster.

---

## Project Structure

```
danish_housing_labour/
├── ingestion/          # Python: DST API → Snowflake raw tables
├── dbt_project/        # dbt Core: staging → core → marts
└── orchestration/      # Dagster: daily refresh schedule
```

---

## Quick Start

### 1. Ingestion

```bash
cd ingestion
pip install -r requirements.txt
cp .env.example .env   # fill in Snowflake credentials
python run_ingestion.py
```

### 2. dbt

```bash
cd dbt_project
pip install dbt-snowflake
dbt deps              # install packages from packages.yml
dbt seed              # load municipality_regions.csv
dbt run               # build all models
dbt test              # run all tests
dbt docs generate && dbt docs serve
```

> **Note:** `profiles.yml` is included in this repo for convenience.
> Move it to `~/.dbt/profiles.yml` for production use.

### 3. Orchestration (Dagster)

```bash
cd orchestration
pip install dagster dagster-dbt
dagster dev           # opens Dagster UI at http://localhost:3000
```

Before running, generate the dbt manifest:

```bash
cd dbt_project && dbt parse
```

---

## Data Sources

| DST Table | Description                          | Snowflake Raw Table    |
|-----------|--------------------------------------|------------------------|
| EJEN12    | Property sale prices per m²          | `RAW_HOUSING_PRICES`   |
| AUL01     | Registered unemployment by area      | `RAW_UNEMPLOYMENT`     |
| LONS10    | Average monthly earnings by industry | `RAW_EARNINGS`         |

---

## Key Models

| Model                            | Layer   | Description                                      |
|----------------------------------|---------|--------------------------------------------------|
| `src_housing_prices`             | Staging | Cast + renamed housing price rows                |
| `src_unemployment`               | Staging | Cast + renamed unemployment rows                 |
| `src_earnings`                   | Staging | Cast + renamed earnings rows                     |
| `dim_municipalities`             | Core    | Municipality ↔ region mapping (from seed)        |
| `fct_housing_transactions`       | Core    | Incremental housing prices enriched with region  |
| `fct_labour_market`              | Core    | Incremental unemployment + earnings per period   |
| `mart_housing_affordability`     | Marts   | Price-to-income ratio per municipality           |
| `mart_labour_housing_correlation`| Marts   | Regional affordability tiers                     |
