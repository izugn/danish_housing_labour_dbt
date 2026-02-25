"""
Fetch sample data from Statistics Denmark (DST) API.

Retrieves the first 100 rows from each of the three DST tables used by this
project and writes them as semicolon-delimited CSV files in this directory.

Tables fetched:
  - EJEN12  -> ejen12_housing_prices.csv    (property sale prices per m²)
  - AUL01   -> aul01_unemployment.csv       (registered unemployment by area)
  - LONS10  -> lons10_earnings.csv          (average monthly earnings by industry)

Dependencies: requests  (already listed in ingestion/requirements.txt)

Usage:
    python data_exploration/fetch_dst_tables.py
"""

import csv
from io import StringIO
from pathlib import Path

import requests
from requests import HTTPError

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DST_API_URL = "https://api.statbank.dk/v1/data"
OUTPUT_DIR = Path(__file__).parent / "data_sample"
SAMPLE_ROWS = 100

TABLES = [
    {
        "table_id": "EJEN12",
        "filename": "ejen12_housing_prices.csv",
        "description": "Property sale prices per m²",
        "variables": [
            {"code": "OMRÅDE", "values": ["*"]},
            {"code": "ENHED",  "values": ["*"]},
            {"code": "Tid",    "values": ["*"]},
        ],
    },
    {
        "table_id": "AUL01",
        "filename": "aul01_unemployment.csv",
        "description": "Registered unemployment by area",
        "variables": [
            {"code": "OMRÅDE", "values": ["*"]},
            {"code": "Tid",    "values": ["*"]},
        ],
    },
    {
        "table_id": "LONS10",
        "filename": "lons10_earnings.csv",
        "description": "Average monthly earnings by industry",
        "variables": [
            {"code": "BRANCHE", "values": ["*"]},
            {"code": "Tid",     "values": ["*"]},
        ],
    },
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def fetch_table_text(table_id: str, variables: list) -> str:
    """POST to the DST /data endpoint and return the raw CSV text."""
    payload = {
        "table": table_id,
        "format": "CSV",
        "lang": "en",
        "variables": variables,
    }
    response = requests.post(DST_API_URL, json=payload, timeout=60)
    try:
        response.raise_for_status()
    except HTTPError as exc:
        details = response.text.strip()
        raise HTTPError(f"{exc} | API response: {details}", response=response) from exc
    return response.text


def fetch_table_info(table_id: str) -> dict:
    """Fetch table metadata used to resolve required variable dimensions."""
    response = requests.get(f"https://api.statbank.dk/v1/tableinfo/{table_id}", params={"format": "JSON"}, timeout=60)
    response.raise_for_status()
    return response.json()


def resolve_variables(table_id: str, configured_variables: list) -> list:
    """Ensure request includes all required variables.

    Keeps configured variable selections as-is and auto-fills any missing
    dimensions with the first available value from table metadata.
    """
    table_info = fetch_table_info(table_id)
    table_variables = table_info.get("variables", [])

    configured_by_code = {var["code"]: var["values"] for var in configured_variables}
    resolved = []

    for variable in table_variables:
        code = variable.get("id")
        if code in configured_by_code:
            resolved.append({"code": code, "values": configured_by_code[code]})
            continue

        values = variable.get("values", [])
        if not values:
            continue

        first_value = values[0].get("id")
        if first_value is None:
            continue

        resolved.append({"code": code, "values": [first_value]})

    return resolved


def normalise_header(name: str) -> str:
    return name.strip().upper().replace(" ", "_")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    for table in TABLES:
        tid = table["table_id"]
        desc = table["description"]
        out_path = OUTPUT_DIR / table["filename"]

        print(f"Fetching {tid} ({desc}) ...", end=" ", flush=True)
        variables = resolve_variables(tid, table["variables"])
        raw_text = fetch_table_text(tid, variables)

        reader = csv.reader(StringIO(raw_text), delimiter=";")
        rows = list(reader)

        if not rows:
            print("WARNING: empty response, skipping.")
            continue

        header = [normalise_header(col) for col in rows[0]]
        data_rows = rows[1:]  # exclude header

        sample = data_rows[:SAMPLE_ROWS]

        with open(out_path, "w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh, delimiter=";")
            writer.writerow(header)
            writer.writerows(sample)

        print(
            f"done.  {len(data_rows):,} total data rows available; "
            f"saved {len(sample)} rows to {out_path.name}"
        )

    print("\nAll tables written to:", OUTPUT_DIR)


if __name__ == "__main__":
    main()
