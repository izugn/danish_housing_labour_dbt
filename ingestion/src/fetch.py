"""
Fetch data from Statistics Denmark (DST) API.
POST requests to https://api.statbank.dk/v1/data
"""
from io import StringIO

import pandas as pd
import requests

from .tables import TableConfig

DST_API_URL = "https://api.statbank.dk/v1/data"


def fetch_table(config: TableConfig, fmt: str = "CSV") -> pd.DataFrame:
    """Fetch a single DST table and return as a DataFrame."""
    payload = {
        "table": config.table_id,
        "format": fmt,
        "lang": "en",
        "variables": [
            {"code": var["code"], "values": var["values"]}
            for var in config.variables
        ],
    }
    response = requests.post(DST_API_URL, json=payload, timeout=60)
    response.raise_for_status()

    df = pd.read_csv(StringIO(response.text), sep=";", decimal=",")
    df.columns = [c.strip().upper().replace(" ", "_") for c in df.columns]
    return df


def fetch_all_tables(configs: list[TableConfig]) -> dict[str, pd.DataFrame]:
    """Fetch all configured DST tables and return a dict keyed by table_id."""
    results: dict[str, pd.DataFrame] = {}
    for config in configs:
        print(f"Fetching {config.table_id} ...")
        results[config.table_id] = fetch_table(config)
    return results
