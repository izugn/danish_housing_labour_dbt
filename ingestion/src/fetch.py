"""
Fetch data from Statistics Denmark (DST) API.
POST requests to https://api.statbank.dk/v1/data
"""
import time
from io import StringIO

import pandas as pd
import requests

from .tables import TableConfig

DST_API_BASE = "https://api.statbank.dk/v1"
DST_API_URL = f"{DST_API_BASE}/data"

_MAX_ATTEMPTS = 2
_RETRY_BACKOFF_S = 5


def fetch_table(config: TableConfig, fmt: str = "CSV") -> pd.DataFrame:
    """Fetch a single DST table and return as a DataFrame.

    Adds audit columns:
        _loaded_at  – UTC timestamp of when the row was fetched
        _src_table  – DST table_id (e.g. "EJEN12")

    Retries once (with a 5-second backoff) on transient HTTP errors.
    Raises RuntimeError with DST error detail on a non-2xx response.
    """
    payload = {
        "table": config.table_id,
        "format": fmt,
        "lang": "en",
        "variables": [
            {"code": var["code"], "values": var["values"]}
            for var in config.variables
        ],
    }

    response = None
    for attempt in range(_MAX_ATTEMPTS):
        if attempt > 0:
            print(f"  WARNING: retrying {config.table_id} (attempt {attempt + 1}) "
                  f"after {_RETRY_BACKOFF_S}s ...")
            time.sleep(_RETRY_BACKOFF_S)

        response = requests.post(DST_API_URL, json=payload, timeout=60)
        try:
            response.raise_for_status()
            break  # success — exit retry loop
        except requests.HTTPError:
            if attempt < _MAX_ATTEMPTS - 1:
                continue  # will retry
            # Final attempt failed — raise a descriptive error
            body_excerpt = response.text[:400]
            raise RuntimeError(
                f"DST API error for table '{config.table_id}': "
                f"HTTP {response.status_code} — {body_excerpt}"
            )

    df = pd.read_csv(StringIO(response.text), sep=";", decimal=",")
    df.columns = [c.strip().upper().replace(" ", "_") for c in df.columns]

    loaded_at = pd.Timestamp.utcnow()
    df["_loaded_at"] = loaded_at
    df["_src_table"] = config.table_id

    return df


def fetch_all_tables(configs: list[TableConfig]) -> dict[str, pd.DataFrame]:
    """Fetch all configured DST tables and return a dict keyed by table_id."""
    results: dict[str, pd.DataFrame] = {}
    for config in configs:
        try:
            df = fetch_table(config)
            data_cols = [c for c in df.columns if not c.startswith("_")]
            print(
                f"✓  {config.table_id:<12}→  {len(df):>6,} rows  |  "
                f"cols: {', '.join(data_cols)}"
            )
            results[config.table_id] = df
        except Exception as exc:
            print(f"✗  {config.table_id:<12}→  ERROR: {exc}")

    return results
