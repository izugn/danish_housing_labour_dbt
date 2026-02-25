"""
Fetch sample data from Statistics Denmark (DST) API.

Retrieves recent rows from each of the three DST tables used by this project
and writes them as semicolon-delimited CSV files, alongside a JSON metadata
file for each table.

Tables fetched:
  - EJEN12  -> ejen12_housing_prices.csv    (property sale prices per m²)
  - AUL01   -> aul01_unemployment.csv       (registered unemployment by area)
  - LONS10  -> lons10_earnings.csv          (average monthly earnings by industry)

Dependencies: requests  (already listed in ingestion/requirements.txt)

Usage:
    # Default: fetch last 12 periods per table
    python data_exploration/fetch_dst_tables.py

    # Fetch last 24 periods
    python data_exploration/fetch_dst_tables.py --periods 24

    # Fetch all available data (slow, large)
    python data_exploration/fetch_dst_tables.py --all-periods

    # Explore a single table interactively
    python data_exploration/fetch_dst_tables.py --explore EJEN12
"""

import argparse
import csv
import json
import sys
from io import StringIO
from pathlib import Path

import requests
from requests import HTTPError

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DST_API_BASE = "https://api.statbank.dk/v1"
OUTPUT_DIR = Path(__file__).parent / "data_sample"

# Cell count threshold at which we warn the user (hard limit is 1,000,000)
CELL_COUNT_WARNING_THRESHOLD = 500_000

TABLES = [
    {
        "table_id": "EJEN12",
        "filename": "ejen12_housing_prices.csv",
        "description": "Property sale prices per m²",
        "variables": [
            {"code": "EJENDOMSKATE", "values": ["*"]},
            {"code": "Tid", "values": None},   # filled in at runtime
        ],
    },
    {
        "table_id": "AUL01",
        "filename": "aul01_unemployment.csv",
        "description": "Registered unemployment by area",
        "variables": [
            {"code": "YDELSESTYPE", "values": ["*"]},
            {"code": "OMRÅDE", "values": ["*"]},
            {"code": "Tid", "values": None},
        ],
    },
    {
        "table_id": "LONS10",
        "filename": "lons10_earnings.csv",
        "description": "Average monthly earnings by industry",
        "variables": [
            {"code": "LØNMÅL", "values": ["*"]},
            {"code": "Tid", "values": None},
        ],
    },
]


# ---------------------------------------------------------------------------
# API helpers — all use POST as recommended by the DST API docs
# ---------------------------------------------------------------------------


def post_json(endpoint: str, payload: dict) -> dict | list:
    """POST a JSON payload to a DST API endpoint and return parsed JSON."""
    url = f"{DST_API_BASE}/{endpoint}"
    response = requests.post(url, json={**payload, "lang": "en", "format": "JSON"}, timeout=60)
    try:
        response.raise_for_status()
    except HTTPError as exc:
        raise HTTPError(
            f"{exc} | URL: {url} | Response: {response.text[:400]}", response=response
        ) from exc
    return response.json()


def fetch_table_metadata(table_id: str) -> dict:
    """
    Fetch full table metadata via POST to /tableinfo.

    Returns a dict with keys: id, text, unit, updated, variables (list).
    Each variable has: id, text, elimination, values (list of {id, text}).
    """
    return post_json("tableinfo", {"table": table_id})


def fetch_table_csv(table_id: str, variables: list[dict]) -> str:
    """POST to /data and return raw CSV text (semicolon-separated)."""
    url = f"{DST_API_BASE}/data"
    payload = {
        "table": table_id,
        "format": "CSV",
        "lang": "en",
        "timeOrder": "Ascending",
        "variables": variables,
    }
    response = requests.post(url, json=payload, timeout=120)
    try:
        response.raise_for_status()
    except HTTPError as exc:
        raise HTTPError(
            f"{exc} | Response: {response.text[:400]}", response=response
        ) from exc
    return response.text


# ---------------------------------------------------------------------------
# Variable resolution
# ---------------------------------------------------------------------------


def pick_last_n_periods(time_values: list[dict], n_periods: int | None) -> list[str]:
    """
    Return the last n_periods values from the metadata time variable.

    - n_periods=None → all periods ("*")
    - n_periods=N    → the last N actual period IDs from the metadata list
    """
    if n_periods is None:
        return ["*"]
    return [v["id"] for v in time_values[-n_periods:]]


def resolve_variables(
    metadata: dict,
    configured: list[dict],
    n_periods: int | None,
) -> list[dict]:
    """
    Merge user-configured variable selections with table metadata.

    Rules:
    - If a variable is in `configured` with explicit values → use them.
    - If a variable has code "Tid" → use period selector.
    - If a variable is eliminable → skip it (DST will aggregate automatically).
    - If a variable is NOT eliminable and not configured → select ALL values ("*")
      and print a warning so the user knows.

    Returns a list of {"code": ..., "values": [...]} dicts ready for the API.
    """
    configured_by_code = {
        v["code"].upper(): v["values"]
        for v in configured
        if v.get("values") is not None
    }
    resolved = []

    for var in metadata.get("variables", []):
        code = var["id"].upper()
        is_time = var.get("time", False)
        eliminable = var.get("elimination", False)

        # Time variable — derive last N from actual period values in metadata
        if is_time:
            period_values = pick_last_n_periods(var.get("values", []), n_periods)
            resolved.append({"code": var["id"], "values": period_values})
            continue

        # Explicitly configured variable
        if code in configured_by_code:
            resolved.append({"code": var["id"], "values": configured_by_code[code]})
            continue

        # Eliminable variables we haven't configured → skip (DST aggregates)
        if eliminable:
            continue

        # Required, not configured → select all values and warn
        print(
            f"  ⚠  Variable '{var['id']}' ({var.get('text', '')}) is required "
            f"but not configured — selecting all {len(var.get('values', []))} values."
        )
        resolved.append({"code": var["id"], "values": ["*"]})

    return resolved


def estimate_cell_count(metadata: dict, variables: list[dict]) -> int:
    """
    Rough cell-count estimate: rows × (n_variables + 1 for INDHOLD).
    DST counts each column (including the value column) as a cell.
    """
    value_counts = []
    var_lookup = {v["id"].upper(): v for v in metadata.get("variables", [])}

    for req_var in variables:
        code = req_var["code"].upper()
        meta_var = var_lookup.get(code, {})
        total_values = len(meta_var.get("values", []))
        selected = req_var["values"]

        if selected == ["*"]:
            value_counts.append(total_values)
        else:
            # explicit list of period IDs (or any other values)
            value_counts.append(min(len(selected), total_values))

    if not value_counts:
        return 0

    n_columns = len(value_counts) + 1  # +1 for INDHOLD (the value column)
    rows = 1
    for vc in value_counts:
        rows *= max(vc, 1)
    return rows * n_columns


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------


def normalise_header(name: str) -> str:
    return name.strip().upper().replace(" ", "_")


def save_metadata(metadata: dict, out_path: Path) -> None:
    """Save a human-readable metadata summary as JSON next to the CSV."""
    summary = {
        "id": metadata.get("id"),
        "title": metadata.get("text"),
        "unit": metadata.get("unit"),
        "last_updated": metadata.get("updated"),
        "variables": [
            {
                "code": v["id"],
                "label": v.get("text"),
                "time": v.get("time", False),
                "eliminable": v.get("elimination", False),
                "n_values": len(v.get("values", [])),
                "sample_values": [
                    {"code": val["id"], "label": val.get("text")}
                    for val in v.get("values", [])[:5]
                ],
            }
            for v in metadata.get("variables", [])
        ],
    }
    meta_path = out_path.with_suffix(".meta.json")
    meta_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"     Metadata → {meta_path.name}")


def parse_and_save_csv(raw_text: str, out_path: Path) -> tuple[int, int]:
    """Parse raw CSV text, normalise headers, write to file. Returns (total_rows, saved_rows)."""
    reader = csv.reader(StringIO(raw_text), delimiter=";")
    rows = list(reader)
    if not rows:
        return 0, 0

    header = [normalise_header(col) for col in rows[0]]
    data_rows = rows[1:]

    with open(out_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh, delimiter=";")
        writer.writerow(header)
        writer.writerows(data_rows)

    return len(data_rows), len(data_rows)


# ---------------------------------------------------------------------------
# Explore mode
# ---------------------------------------------------------------------------


def explore_table(table_id: str) -> None:
    """Print a detailed variable/value breakdown for a table."""
    print(f"\n{'='*60}")
    print(f"  Exploring table: {table_id.upper()}")
    print(f"{'='*60}")

    meta = fetch_table_metadata(table_id.upper())
    print(f"  Title      : {meta.get('text')}")
    print(f"  Unit       : {meta.get('unit')}")
    print(f"  Updated    : {meta.get('updated')}")
    print(f"  Contact    : {', '.join(c.get('mail', '') for c in meta.get('contacts', []))}")
    print()

    for var in meta.get("variables", []):
        values = var.get("values", [])
        flags = []
        if var.get("time"):
            flags.append("TIME")
        if var.get("elimination"):
            flags.append("ELIMINABLE")
        flag_str = f"  [{', '.join(flags)}]" if flags else ""

        print(f"  Variable: {var['id']} — {var.get('text')}{flag_str}")
        print(f"    Total values: {len(values)}")
        for v in values[:8]:
            print(f"      {v['id']:>10}  {v.get('text', '')}")
        if len(values) > 8:
            print(f"      ... and {len(values) - 8} more")
        print()


# ---------------------------------------------------------------------------
# Main fetch loop
# ---------------------------------------------------------------------------


def main(n_periods: int | None = 12, all_periods: bool = False, explore: str | None = None) -> None:

    if explore:
        explore_table(explore)
        return

    effective_periods = None if all_periods else n_periods
    period_label = "all periods" if effective_periods is None else f"last {effective_periods} periods"

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    print(f"\nDST Data Exploration — fetching {period_label}\n")

    results_summary = []

    for table in TABLES:
        tid = table["table_id"]
        out_path = OUTPUT_DIR / table["filename"]

        print(f"{'─'*50}")
        print(f"  Table   : {tid} — {table['description']}")

        # 1. Fetch metadata
        print(f"  Fetching metadata ...", end=" ", flush=True)
        meta = fetch_table_metadata(tid)
        print(f"✓  (updated: {meta.get('updated', 'unknown')})")
        print(f"  Title   : {meta.get('text')}")
        print(f"  Unit    : {meta.get('unit')}")

        # 2. Resolve variables
        resolved = resolve_variables(meta, table["variables"], effective_periods)
        print(f"  Variables selected:")
        for v in resolved:
            print(f"    {v['code']:>15} = {v['values']}")

        # 3. Estimate cell count and warn if large
        est_cells = estimate_cell_count(meta, resolved)
        if est_cells > CELL_COUNT_WARNING_THRESHOLD:
            print(
                f"  ⚠  Estimated cell count: {est_cells:,} "
                f"(DST hard limit: 1,000,000). Consider reducing --periods."
            )
        else:
            print(f"  Estimated cells: ~{est_cells:,}")

        # 4. Fetch data
        print(f"  Fetching data ...", end=" ", flush=True)
        try:
            raw_text = fetch_table_csv(tid, resolved)
        except HTTPError as exc:
            print(f"\n  ERROR: {exc}")
            results_summary.append({"table": tid, "status": "ERROR", "rows": 0})
            continue

        # 5. Parse and save
        total_rows, saved_rows = parse_and_save_csv(raw_text, out_path)
        print(f"✓  {total_rows:,} rows")
        print(f"     Data     → {out_path.name}")
        save_metadata(meta, out_path)

        results_summary.append({"table": tid, "status": "OK", "rows": saved_rows})

    # Final summary
    print(f"\n{'='*50}")
    print("  SUMMARY")
    print(f"{'='*50}")
    for r in results_summary:
        status_icon = "✓" if r["status"] == "OK" else "✗"
        print(f"  {status_icon}  {r['table']:8}  {r['rows']:>8,} rows   [{r['status']}]")
    print(f"\n  Output directory: {OUTPUT_DIR}\n")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Fetch sample data from the Statistics Denmark (DST) StatBank API."
    )
    parser.add_argument(
        "--periods",
        type=int,
        default=12,
        metavar="N",
        help="Number of recent time periods to fetch per table (default: 12).",
    )
    parser.add_argument(
        "--all-periods",
        action="store_true",
        help="Fetch all available periods (ignores --periods). May be large.",
    )
    parser.add_argument(
        "--explore",
        metavar="TABLE_ID",
        help="Print a detailed variable/value breakdown for a table and exit.",
    )
    args = parser.parse_args()

    try:
        main(n_periods=args.periods, all_periods=args.all_periods, explore=args.explore)
    except KeyboardInterrupt:
        print("\nAborted.")
        sys.exit(1)
