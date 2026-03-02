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


def _get_variable_values(table_id: str, variable_code: str) -> list[str]:
    """Return all available value IDs for a variable by calling the DST tableinfo endpoint."""
    response = requests.get(
        f"{DST_API_BASE}/tableinfo/{table_id}",
        params={"lang": "en"},
        timeout=30,
    )
    response.raise_for_status()
    for var in response.json()["variables"]:
        if var["id"].upper() == variable_code.upper():
            return [v["id"] for v in var["values"]]
    raise ValueError(f"Variable '{variable_code}' not found in table '{table_id}'")


def _fetch_single(config: TableConfig, variables: list[dict], fmt: str) -> pd.DataFrame:
    """Send one POST request to the DST data endpoint and return a DataFrame."""
    payload = {
        "table": config.table_id,
        "format": fmt,
        "lang": "en",
        "variables": variables,
    }

    response = None
    for attempt in range(_MAX_ATTEMPTS):
        if attempt > 0:
            print(
                f"  WARNING: retrying {config.table_id} (attempt {attempt + 1}) "
                f"after {_RETRY_BACKOFF_S}s ..."
            )
            time.sleep(_RETRY_BACKOFF_S)

        response = requests.post(DST_API_URL, json=payload, timeout=60)
        try:
            response.raise_for_status()
            break
        except requests.HTTPError:
            if attempt < _MAX_ATTEMPTS - 1:
                continue
            body_excerpt = response.text[:400]
            raise RuntimeError(
                f"DST API error for table '{config.table_id}': "
                f"HTTP {response.status_code} — {body_excerpt}"
            )

    df = pd.read_csv(StringIO(response.text), sep=";", decimal=",")
    df.columns = [c.strip().upper().replace(" ", "_") for c in df.columns]
    return df


def fetch_table(config: TableConfig, fmt: str = "CSV") -> pd.DataFrame:
    """Fetch a single DST table and return as a DataFrame.

    Adds audit columns:
        _loaded_at  – UTC timestamp of when the row was fetched
        _src_table  – DST table_id (e.g. "EJEN12")

    Retries once (with a 5-second backoff) on transient HTTP errors.
    Raises RuntimeError with DST error detail on a non-2xx response.

    If ``config.chunk_variable`` is set, the request is split into multiple
    sub-requests (each covering ``config.chunk_size`` values of that variable)
    to stay under the DST 1 M-cell CSV limit.  Results are concatenated before
    the audit columns are added.
    """
    base_variables = [
        {"code": var["code"], "values": var["values"]}
        for var in config.variables
    ]

    if config.chunk_variable:
        all_values = _get_variable_values(config.table_id, config.chunk_variable)
        chunks = [
            all_values[i : i + config.chunk_size]
            for i in range(0, len(all_values), config.chunk_size)
        ]
        print(
            f"  ↳ chunking {config.table_id} by '{config.chunk_variable}' "
            f"into {len(chunks)} sub-requests ({config.chunk_size} values each) ..."
        )
        frames: list[pd.DataFrame] = []
        for idx, chunk_values in enumerate(chunks, start=1):
            chunk_variables = [
                (
                    {"code": var["code"], "values": chunk_values}
                    if var["code"].upper() == config.chunk_variable.upper()
                    else var
                )
                for var in base_variables
            ]
            frame = _fetch_single(config, chunk_variables, fmt)
            frames.append(frame)
            print(f"    chunk {idx}/{len(chunks)}: {len(frame):,} rows")
        df = pd.concat(frames, ignore_index=True)
    else:
        df = _fetch_single(config, base_variables, fmt)

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
